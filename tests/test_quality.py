from collections import Counter
from datetime import UTC, date, datetime, timedelta

import pytest
from raw_fixtures import reversal_of, table_2019_era, trip, write

from lakehouse.config import Month
from lakehouse.quality import RULE_CODES, classify
from lakehouse.schema import canonicalize

MONTH = Month(2019, 1)
T0 = datetime(2019, 1, 15, 8, 0)


def run(spark, tmp_path, rows):
    raw = spark.read.parquet(write(table_2019_era(rows), tmp_path / "raw.parquet"))
    df = canonicalize(
        raw,
        source_month=date(2019, 1, 1),
        processed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    return classify(df, MONTH).collect()


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, []),
        (
            {
                "tpep_pickup_datetime": datetime(2018, 12, 31, 23, 59),
                "tpep_dropoff_datetime": datetime(2019, 1, 1, 0, 10),
            },
            ["pickup_outside_source_month"],
        ),
        (
            {
                "tpep_pickup_datetime": datetime(2019, 2, 1, 0, 0),
                "tpep_dropoff_datetime": datetime(2019, 2, 1, 0, 5),
            },
            ["pickup_outside_source_month"],
        ),
        ({"tpep_dropoff_datetime": T0 - timedelta(minutes=1)}, ["dropoff_before_pickup"]),
        ({"tpep_dropoff_datetime": T0 + timedelta(hours=24)}, []),
        ({"tpep_dropoff_datetime": T0 + timedelta(hours=24, seconds=1)}, ["duration_over_24h"]),
        ({"trip_distance": -0.1}, ["negative_distance"]),
        ({"trip_distance": 500.0}, []),
        ({"trip_distance": 500.1}, ["distance_over_500_miles"]),
        ({"PULocationID": 0}, ["invalid_location_id"]),
        ({"DOLocationID": 266}, ["invalid_location_id"]),
        ({"trip_distance": None}, ["missing_required_field"]),
        ({"fare_amount": -5.0}, ["negative_amount_unmatched"]),
        (
            {
                "tpep_pickup_datetime": datetime(2019, 2, 3),
                "tpep_dropoff_datetime": datetime(2019, 2, 2),
            },
            ["pickup_outside_source_month", "dropoff_before_pickup"],
        ),
    ],
)
def test_single_row_rules(spark, tmp_path, overrides, expected):
    (row,) = run(spark, tmp_path, [trip(**overrides)])
    assert row.reject_reasons == expected
    assert row.reject_reason == (expected[0] if expected else None)


def test_reversal_quarantines_both_halves(spark, tmp_path):
    original = trip()
    rows = run(spark, tmp_path, [original, reversal_of(original), trip(PULocationID=100)])
    assert Counter(r.reject_reason for r in rows) == Counter(
        {"reversal_negative_row": 1, "reversed_by_negative_row": 1, None: 1}
    )


def test_partial_negation_is_not_a_reversal(spark, tmp_path):
    original = trip()
    partial = {**reversal_of(original), "tip_amount": 3.0}  # tip left positive
    rows = run(spark, tmp_path, [original, partial])
    assert Counter(r.reject_reason for r in rows) == Counter(
        {"negative_amount_unmatched": 1, None: 1}
    )


def test_reversal_pairs_one_to_one(spark, tmp_path):
    original = trip()
    rows = run(spark, tmp_path, [original, original, reversal_of(original)])
    assert Counter(r.reject_reason for r in rows) == Counter(
        {"reversal_negative_row": 1, "reversed_by_negative_row": 1, None: 1}
    )


def test_exact_duplicates_keep_one_copy(spark, tmp_path):
    rows = run(spark, tmp_path, [trip(), trip(), trip()])
    assert Counter(r.reject_reason for r in rows) == Counter({None: 1, "duplicate_row": 2})


def test_every_documented_rule_is_reachable():
    assert len(RULE_CODES) == len(set(RULE_CODES)) == 11
