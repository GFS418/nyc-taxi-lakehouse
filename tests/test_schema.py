import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from pyspark.sql import types as T
from raw_fixtures import table_2019_era, table_2023_era, trip, write

from lakehouse.schema import CORE_COLUMNS, SOFT_FLAG_COLUMNS, SchemaContractError, canonicalize

CONTRACT = Path(__file__).resolve().parents[1] / "schemas" / "bigquery"
KW = dict(
    source_month=date(2019, 1, 1),
    processed_at=datetime(2026, 9, 11, tzinfo=UTC),
)

# How BigQuery types each Spark type after a Parquet load (per the BigQuery conversion table).
SPARK_TO_BQ = {
    T.LongType: "INT64",
    T.DoubleType: "FLOAT64",
    T.StringType: "STRING",
    T.BooleanType: "BOOL",
    T.DecimalType: "NUMERIC",
    T.TimestampNTZType: "TIMESTAMP",
    T.TimestampType: "TIMESTAMP",
    T.DateType: "DATE",
}


def canonical(spark, tmp_path, table, **kw):
    return canonicalize(spark.read.parquet(write(table, tmp_path / "raw.parquet")), **{**KW, **kw})


def test_both_schema_eras_produce_identical_schemas(spark, tmp_path):
    old = canonical(spark, tmp_path / "a", table_2019_era([trip()]))
    new = canonical(spark, tmp_path / "b", table_2023_era([trip()]))
    assert old.schema.simpleString() == new.schema.simpleString()
    assert tuple(old.columns) == CORE_COLUMNS


def test_canonical_schema_matches_bigquery_contract(spark, tmp_path):
    df = canonical(spark, tmp_path, table_2019_era([trip()]))
    contract = json.loads((CONTRACT / "yellow_trips.json").read_text())
    ours = [(f.name, SPARK_TO_BQ[type(f.dataType)]) for f in df.schema.fields]
    assert ours == [(f["name"], f["type"]) for f in contract[: len(ours)]]
    # The soft flags come after the canonical columns, and are added by the quality step.
    assert [(f["name"], f["type"]) for f in contract[len(ours) :]] == [
        (c, "BOOL") for c in SOFT_FLAG_COLUMNS
    ]


def test_fills_follow_the_decisions(spark, tmp_path):
    rows = [trip(passenger_count=None, RatecodeID=None, congestion_surcharge=None)]
    out = canonical(spark, tmp_path, table_2019_era(rows)).collect()[0]
    assert out.passenger_count is None  # unrecorded stays NULL, never filled
    assert out.ratecode_id == 99
    assert out.congestion_surcharge == Decimal("0.00")
    assert out.airport_fee == Decimal("0.00")  # null-typed column in 2019-era files
    assert out.cbd_congestion_fee == Decimal("0.00")  # column absent before 2025


def test_large_amount_survives_the_cast_so_it_can_be_quarantined(spark, tmp_path):
    rows = [trip(fare_amount=-133_391_414.0)]
    out = canonical(spark, tmp_path, table_2019_era(rows)).collect()[0]
    assert out.fare_amount == Decimal("-133391414.00")


def test_money_is_exact_decimal(spark, tmp_path):
    out = canonical(spark, tmp_path, table_2019_era([trip(fare_amount=0.1 + 0.2)])).collect()[0]
    assert out.fare_amount == Decimal("0.30")


def test_trip_id_keeps_null_and_zero_passengers_distinct(spark, tmp_path):
    rows = [trip(passenger_count=None), trip(passenger_count=0)]
    ids = [r.trip_id for r in canonical(spark, tmp_path, table_2019_era(rows)).collect()]
    assert len(set(ids)) == 2


def test_unknown_source_column_breaks_the_contract(spark, tmp_path):
    table = table_2023_era([trip()]).append_column("surprise_fee", pa.array([1.0]))
    with pytest.raises(SchemaContractError, match="surprise_fee"):
        canonical(spark, tmp_path, table)


def test_missing_required_column_breaks_the_contract(spark, tmp_path):
    table = table_2023_era([trip()]).drop_columns(["tip_amount"])
    with pytest.raises(SchemaContractError, match="tip_amount"):
        canonical(spark, tmp_path, table)
