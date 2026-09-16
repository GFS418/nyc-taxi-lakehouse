"""The canonical trip schema: the one contract every monthly TLC file is coerced into.

TLC files drift between months: int64 vs int32 IDs, double vs int64 counts, airport_fee vs
Airport_fee, a null-typed airport_fee before 2021, and cbd_congestion_fee only from 2025.
This module maps every known variant onto one schema and fails loudly on anything unknown,
so a new or renamed source column stops the pipeline instead of silently vanishing.
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 15 digits, not 10: 2022-12 contains an amount of -133,391,414. The amount_out_of_range
# rule quarantines values like that, but the type has to represent them first.
MONEY = T.DecimalType(15, 2)

# lowercase source column -> (canonical column, canonical type). Order is the output order.
SOURCE_COLUMNS: dict[str, tuple[str, T.DataType]] = {
    "vendorid": ("vendor_id", T.LongType()),
    "tpep_pickup_datetime": ("pickup_datetime", T.TimestampNTZType()),
    "tpep_dropoff_datetime": ("dropoff_datetime", T.TimestampNTZType()),
    "passenger_count": ("passenger_count", T.LongType()),
    "trip_distance": ("trip_distance", T.DoubleType()),
    "ratecodeid": ("ratecode_id", T.LongType()),
    "store_and_fwd_flag": ("store_and_fwd_flag", T.StringType()),
    "pulocationid": ("pu_location_id", T.LongType()),
    "dolocationid": ("do_location_id", T.LongType()),
    "payment_type": ("payment_type", T.LongType()),
    "fare_amount": ("fare_amount", MONEY),
    "extra": ("extra", MONEY),
    "mta_tax": ("mta_tax", MONEY),
    "tip_amount": ("tip_amount", MONEY),
    "tolls_amount": ("tolls_amount", MONEY),
    "improvement_surcharge": ("improvement_surcharge", MONEY),
    "total_amount": ("total_amount", MONEY),
    "congestion_surcharge": ("congestion_surcharge", MONEY),
    "airport_fee": ("airport_fee", MONEY),
    "cbd_congestion_fee": ("cbd_congestion_fee", MONEY),
}
OPTIONAL_SOURCE_COLUMNS = frozenset({"airport_fee", "cbd_congestion_fee"})
BUSINESS_COLUMNS = tuple(canon for canon, _ in SOURCE_COLUMNS.values())
MONEY_COLUMNS = tuple(canon for canon, dtype in SOURCE_COLUMNS.values() if dtype == MONEY)
NULL_TO_ZERO_FEES = ("congestion_surcharge", "airport_fee", "cbd_congestion_fee")
RATECODE_UNKNOWN = 99  # TLC dictionary: 99 = Null/unknown

TRIP_COLUMNS = (
    "trip_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    *MONEY_COLUMNS,
    "source_month",
    "processed_at",
)
QUARANTINE_COLUMNS = (*TRIP_COLUMNS, "reject_reason", "reject_reasons")


class SchemaContractError(ValueError):
    """A source file has columns the contract does not know, or lacks required ones."""


def fingerprint(columns: tuple[str, ...]) -> Column:
    """MD5 over all columns; the delimiter and NUL token keep NULL, '' and 0 distinct."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\x00")) for c in columns]
    return F.md5(F.concat_ws("\x1f", *parts))


def canonicalize(raw: DataFrame, *, source_month: date, processed_at: datetime) -> DataFrame:
    by_lower: dict[str, str] = {}
    for name in raw.columns:
        key = name.lower()
        if key in by_lower:
            raise SchemaContractError(f"columns {by_lower[key]!r} and {name!r} collide")
        by_lower[key] = name
    unknown = sorted(set(by_lower) - set(SOURCE_COLUMNS))
    missing = sorted(set(SOURCE_COLUMNS) - OPTIONAL_SOURCE_COLUMNS - set(by_lower))
    if unknown or missing:
        raise SchemaContractError(f"unknown columns: {unknown}; missing required: {missing}")

    typed = raw.select(
        *[
            (F.col(f"`{by_lower[src]}`") if src in by_lower else F.lit(None))
            .cast(dtype)
            .alias(canon)
            for src, (canon, dtype) in SOURCE_COLUMNS.items()
        ]
    )
    # Identity comes from the values as published, before any fill, so a filled NULL can
    # never collide with a genuine 0 and be removed as a "duplicate".
    typed = typed.withColumn("trip_id", fingerprint(BUSINESS_COLUMNS))

    # passenger_count stays NULL when unrecorded: a 0 would turn 11.6% of 2024-06 trips into
    # zero-passenger trips. Rate code NULLs map to 99, the TLC dictionary's own "unknown".
    filled = typed.withColumn(
        "ratecode_id", F.coalesce("ratecode_id", F.lit(RATECODE_UNKNOWN).cast("bigint"))
    )
    for fee in NULL_TO_ZERO_FEES:
        filled = filled.withColumn(fee, F.coalesce(fee, F.lit(0).cast(MONEY)))
    return filled.select(
        *[c for c in TRIP_COLUMNS if c not in ("source_month", "processed_at")],
        F.lit(source_month).alias("source_month"),
        F.lit(processed_at).alias("processed_at"),
    )
