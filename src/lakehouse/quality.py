"""Hard data-quality rules, applied by Spark before anything reaches the warehouse.

A hard rule marks a row as unusable. Failing rows are quarantined with every rule they
failed, in precedence order; nothing is dropped silently.

A soft flag marks a row as suspicious but possible: zero distance, zero duration, a meter left
running for hours, a near-duplicate. Flagged rows stay in the curated data with a boolean per
flag, so an analyst can exclude them deliberately instead of never having seen them.

Unknown code values (a new vendor or payment type) are contract drift for a human to review, so
dbt tests catch them, not Spark.
"""

from __future__ import annotations

import operator
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.config import Month
from lakehouse.schema import CLASSIFIED_COLUMNS, MONEY_COLUMNS

MAX_DISTANCE_MILES = 500
MAX_ABS_AMOUNT = 10_000
# The 99.9th percentile of implied speed is about 50 mph across four profiled months, and the
# rows above 100 are not fast trips: the median one covers 2.7 miles in 13 seconds.
MAX_SPEED_MPH = 100
LONG_DURATION = "INTERVAL '3' HOUR"
LOCATION_ID_RANGE = (1, 265)
REQUIRED_NON_NULL = (
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "trip_distance",
    "fare_amount",
    "total_amount",
)
REVERSAL_KEY = (
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
)
# Same trip, same fare, but different surcharges or tips: likely a resubmission, and there is no
# submission timestamp to say which version is right. Flagged, never removed.
NEAR_DUPLICATE_KEY = (*REVERSAL_KEY, "fare_amount")


@dataclass(frozen=True)
class Rule:
    code: str
    description: str


RULES = (
    Rule("missing_required_field", f"NULL in any of {', '.join(REQUIRED_NON_NULL)}."),
    Rule("invalid_location_id", "Pickup or dropoff zone outside TLC zone IDs 1-265."),
    Rule(
        "pickup_outside_source_month",
        "Pickup is not inside the month of the file it came from (late, misfiled, or bad clock).",
    ),
    Rule("dropoff_before_pickup", "Dropoff timestamp earlier than pickup."),
    Rule("duration_over_24h", "Meter engaged for more than 24 hours."),
    Rule("negative_distance", "Negative trip distance."),
    Rule("distance_over_500_miles", f"Trip distance above {MAX_DISTANCE_MILES} miles."),
    Rule(
        "implied_speed_over_100_mph",
        f"Distance over duration above {MAX_SPEED_MPH} mph, or any distance in zero time.",
    ),
    Rule(
        "amount_out_of_range",
        f"A monetary column beyond ${MAX_ABS_AMOUNT:,} in absolute value; no real trip costs that.",
    ),
    Rule(
        "reversal_negative_row",
        "Negative-amount row that exactly negates another row of the same trip (a void or refund).",
    ),
    Rule(
        "reversed_by_negative_row",
        "The original charge that a reversal_negative_row cancels; the pair nets to zero.",
    ),
    Rule("negative_amount_unmatched", "Any negative amount with no exact reversal partner."),
    Rule("duplicate_row", "Exact duplicate of a row that passed every rule; one copy is kept."),
)
RULE_CODES = tuple(rule.code for rule in RULES)

SOFT_FLAGS = (
    Rule("is_zero_distance", "Trip distance is exactly zero: a cancelled trip or a flag drop."),
    Rule("is_zero_duration", "Dropoff equals pickup, with zero distance."),
    Rule("is_long_duration", "Meter engaged more than 3 hours, typically left running."),
    Rule(
        "is_near_duplicate",
        "Shares vendor, timestamps, zones and fare with another kept row, but differs elsewhere.",
    ),
)


def _any(conditions: list[Column]) -> Column:
    return reduce(operator.or_, conditions)


def _with_reversal_flags(df: DataFrame) -> DataFrame:
    """Pair each negative row one-to-one with a non-negative row whose amounts it exactly negates.

    Both halves of a pair share a signature: the trip identity plus the original's amounts
    (a negative row contributes its amounts negated). Ranking each side inside the signature
    and pairing rank n with rank n gives a strict one-to-one match with a single window pass.
    """
    is_negative = F.coalesce(_any([F.col(c) < 0 for c in MONEY_COLUMNS]), F.lit(False))
    df = df.withColumn("_is_negative", is_negative)
    sig_cols = [f"_sig_{c}" for c in MONEY_COLUMNS]
    df = df.select(
        "*",
        *[
            F.when(F.col("_is_negative"), -F.col(c)).otherwise(F.col(c)).alias(s)
            for c, s in zip(MONEY_COLUMNS, sig_cols, strict=True)
        ],
    )
    signature = [*REVERSAL_KEY, *sig_cols]
    side = Window.partitionBy(*signature, "_is_negative").orderBy("trip_id")
    group = Window.partitionBy(*signature)
    return (
        df.withColumn("_side_rank", F.row_number().over(side))
        .withColumn("_n_neg", F.sum(F.col("_is_negative").cast("int")).over(group))
        .withColumn("_n_pos", F.sum((~F.col("_is_negative")).cast("int")).over(group))
        .withColumn("_paired", F.col("_side_rank") <= F.least("_n_neg", "_n_pos"))
    )


def _rule_conditions(month: Month) -> dict[str, Column]:
    start = F.lit(f"{month.first_day} 00:00:00").cast("timestamp_ntz")
    end = F.lit(f"{month.next().first_day} 00:00:00").cast("timestamp_ntz")
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")
    # The session time zone is pinned to UTC, so casting wall-clock values to TIMESTAMP is exact.
    seconds = F.unix_seconds(dropoff.cast("timestamp")) - F.unix_seconds(pickup.cast("timestamp"))
    distance = F.col("trip_distance")
    lo, hi = LOCATION_ID_RANGE
    return {
        "missing_required_field": _any([F.col(c).isNull() for c in REQUIRED_NON_NULL]),
        "invalid_location_id": ~F.col("pu_location_id").between(lo, hi)
        | ~F.col("do_location_id").between(lo, hi),
        "pickup_outside_source_month": (pickup < start) | (pickup >= end),
        "dropoff_before_pickup": dropoff < pickup,
        "duration_over_24h": (dropoff - pickup) > F.expr("INTERVAL '24' HOUR"),
        "negative_distance": F.col("trip_distance") < 0,
        "distance_over_500_miles": F.col("trip_distance") > MAX_DISTANCE_MILES,
        "implied_speed_over_100_mph": (
            (seconds > 0) & (distance / (seconds / 3600) > MAX_SPEED_MPH)
        )
        | ((seconds == 0) & (distance > 0)),
        "amount_out_of_range": _any([F.abs(F.col(c)) > MAX_ABS_AMOUNT for c in MONEY_COLUMNS]),
        "reversal_negative_row": F.col("_is_negative") & F.col("_paired"),
        "reversed_by_negative_row": ~F.col("_is_negative") & F.col("_paired"),
        "negative_amount_unmatched": F.col("_is_negative") & ~F.col("_paired"),
    }


def classify(df: DataFrame, month: Month) -> DataFrame:
    """Add reject_reasons (every failed rule, precedence order) and reject_reason (the first).

    reject_reason IS NULL means the row passed every rule.
    """
    conditions = _rule_conditions(month)
    assert tuple(conditions) == RULE_CODES[:-1], "rule conditions out of sync with RULES"
    df = _with_reversal_flags(df)
    reasons = F.array_compact(
        F.array(*[F.when(cond, F.lit(code)) for code, cond in conditions.items()])
    )
    df = df.withColumn("reject_reasons", reasons).withColumn(
        "_passes", F.size("reject_reasons") == 0
    )
    # Exact duplicates share a trip_id. Identical rows are interchangeable, so which copy is
    # kept cannot change the output.
    copy_rank = F.row_number().over(Window.partitionBy("trip_id", "_passes").orderBy("trip_id"))
    df = df.withColumn(
        "reject_reasons",
        F.when(F.col("_passes") & (copy_rank > 1), F.array(F.lit("duplicate_row"))).otherwise(
            F.col("reject_reasons")
        ),
    )
    # try_element_at: under ANSI mode (Spark 4 default) element_at on an empty array raises.
    df = df.withColumn("reject_reason", F.try_element_at("reject_reasons", F.lit(1)))
    return _with_soft_flags(df).select(*CLASSIFIED_COLUMNS)


def _with_soft_flags(df: DataFrame) -> DataFrame:
    kept = F.col("reject_reason").isNull()
    group_size = F.count(F.lit(1)).over(Window.partitionBy(*NEAR_DUPLICATE_KEY, "_kept"))
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")
    return (
        df.withColumn("_kept", kept)
        .withColumn("is_zero_distance", F.col("trip_distance") == 0)
        .withColumn("is_zero_duration", dropoff == pickup)
        .withColumn("is_long_duration", (dropoff - pickup) > F.expr(LONG_DURATION))
        .withColumn("is_near_duplicate", F.col("_kept") & (group_size > 1))
    )
