import json
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from raw_fixtures import reversal_of, table_2019_era, table_2023_era, trip, write

from lakehouse.config import Month, dq_report_uri, raw_file_uri
from lakehouse.schema import SchemaContractError
from lakehouse.transform import run_month

MONTH = Month(2019, 1)


def rows():
    good = [
        trip(
            tpep_pickup_datetime=datetime(2019, 1, 2, 9, i),
            tpep_dropoff_datetime=datetime(2019, 1, 2, 9, i) + timedelta(minutes=15),
        )
        for i in range(5)
    ]
    return good + [
        good[0],  # exact duplicate
        trip(tpep_pickup_datetime=datetime(2018, 12, 31, 23, 0)),  # out of period
        trip(PULocationID=100, passenger_count=None),  # passes; passenger count stays NULL
        reversal_of(trip(PULocationID=50)),
        trip(PULocationID=50),
    ]


def seed_raw(root, table):
    write(table, __import__("pathlib").Path(raw_file_uri(root, MONTH)))


def test_month_reconciles_and_reports(spark, tmp_path):
    root = str(tmp_path)
    seed_raw(root, table_2019_era(rows()))
    report = run_month(spark, MONTH, root)
    assert report["source_rows"] == 10
    assert report["curated_rows"] == 6
    assert report["quarantined_rows"] == 4
    assert report["reject_reason_counts"] == {
        "duplicate_row": 1,
        "pickup_outside_source_month": 1,
        "reversal_negative_row": 1,
        "reversed_by_negative_row": 1,
    }
    assert report["passenger_count_null_rows"] == 1
    assert set(report["soft_flag_counts"]) == {
        "is_zero_distance",
        "is_zero_duration",
        "is_long_duration",
        "is_near_duplicate",
    }
    curated = pq.read_table(tmp_path / "curated/yellow/year=2019/month=01")
    assert curated.column("passenger_count").null_count == 1
    assert json.loads(open(dq_report_uri(root, MONTH)).read()) == report


def test_rerun_replaces_output_instead_of_appending(spark, tmp_path):
    root = str(tmp_path)
    seed_raw(root, table_2019_era(rows()))
    first = run_month(spark, MONTH, root)
    second = run_month(spark, MONTH, root)
    curated = pq.read_table(tmp_path / "curated/yellow/year=2019/month=01")
    assert first["curated_rows"] == second["curated_rows"] == curated.num_rows == 6
    assert len(set(curated.column("trip_id").to_pylist())) == 6


def test_quarantine_output_carries_reason_arrays(spark, tmp_path):
    root = str(tmp_path)
    seed_raw(root, table_2023_era(rows()))
    run_month(spark, MONTH, root)
    q = pq.read_table(tmp_path / "quarantine/yellow/year=2019/month=01")
    assert pa.types.is_list(q.schema.field("reject_reasons").type)
    assert all(r for r in q.column("reject_reasons").to_pylist())


def test_failed_run_removes_stale_report_so_it_cannot_be_loaded(spark, tmp_path):
    root = str(tmp_path)
    seed_raw(root, table_2019_era(rows()))
    run_month(spark, MONTH, root)
    bad = table_2019_era(rows()).append_column("surprise_fee", pa.array([1.0] * 10))
    seed_raw(root, bad)
    with pytest.raises(SchemaContractError):
        run_month(spark, MONTH, root)
    assert not (tmp_path / "reports/yellow/year=2019/month=01/dq_report.json").exists()


def test_written_parquet_uses_types_bigquery_loads_cleanly(spark, tmp_path):
    """Pin the physical Parquet types the BigQuery load depends on."""
    root = str(tmp_path)
    seed_raw(root, table_2019_era(rows()))
    run_month(spark, MONTH, root)
    part = next(Path(tmp_path, "curated/yellow/year=2019/month=01").glob("*.parquet"))
    schema = pq.ParquetFile(part).schema
    cols = {schema.column(i).name: schema.column(i) for i in range(len(schema))}

    def logical(name):
        return json.loads(cols[name].logical_type.to_json())

    assert cols["pickup_datetime"].physical_type == "INT64"
    assert logical("pickup_datetime")["isAdjustedToUTC"] is False  # wall-clock, no zone
    assert cols["processed_at"].physical_type == "INT64"  # not deprecated INT96
    assert logical("processed_at")["isAdjustedToUTC"] is True  # a real instant
    assert logical("fare_amount")["Type"] == "Decimal"
    assert cols["source_month"].physical_type == "INT32"


def test_written_outputs_match_the_warehouse_contracts(spark, tmp_path):
    """Curated carries soft flags, quarantine carries reasons. Both must match BigQuery."""
    root = str(tmp_path)
    seed_raw(root, table_2019_era(rows()))
    run_month(spark, MONTH, root)
    contracts = Path(__file__).resolve().parents[1] / "schemas" / "bigquery"
    pairs = [("curated", "yellow_trips.json"), ("quarantine", "yellow_trips_quarantine.json")]
    for folder, contract in pairs:
        written = pq.read_table(Path(tmp_path, folder, "yellow/year=2019/month=01")).schema.names
        expected = [f["name"] for f in json.loads((contracts / contract).read_text())]
        assert written == expected, folder
