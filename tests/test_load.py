import json

import pytest
from google.cloud import bigquery

from lakehouse import load
from lakehouse.config import Month

MONTH = Month(2019, 1)


class FakeJob:
    def __init__(self, rows):
        self.output_rows = rows

    def result(self):
        return self


class FakeClient:
    def __init__(self, rows_by_table):
        self.rows_by_table, self.loads, self.deletes = rows_by_table, [], []

    def load_table_from_uri(self, uri, dest, job_config):
        self.loads.append((uri, dest, job_config))
        return FakeJob(self.rows_by_table[dest.split(".")[-1].split("$")[0]])

    def delete_table(self, dest, not_found_ok):
        self.deletes.append(dest)


@pytest.fixture
def report(monkeypatch):
    data = {"month": "2019-01", "curated_rows": 100, "quarantined_rows": 7}
    monkeypatch.setattr(load, "read_json", lambda uri: data)
    return data


def test_job_config_is_parquet_truncate_into_month_partitions():
    cfg = load.job_config(load.TABLES[0])
    assert cfg.source_format == bigquery.SourceFormat.PARQUET
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert cfg.time_partitioning.type_ == bigquery.TimePartitioningType.MONTH
    assert cfg.time_partitioning.field == "pickup_datetime"
    assert cfg.decimal_target_types is None  # rejected by BigQuery alongside a schema
    assert {f.name: f.field_type for f in cfg.schema}["fare_amount"] == "NUMERIC"
    assert cfg.parquet_options.enable_list_inference is True


@pytest.mark.parametrize("spec", load.TABLES, ids=lambda s: s.table)
def test_job_schema_matches_committed_contract(spec):
    committed = json.loads((load.SCHEMA_DIR / spec.schema_file).read_text())
    assert [f.name for f in load.job_config(spec).schema] == [f["name"] for f in committed]


def test_loads_each_table_through_its_month_partition_decorator(report):
    client = FakeClient({"yellow_trips": 100, "yellow_trips_quarantine": 7})
    result = load.load_month(MONTH, root="gs://lake", project="p", client=client)
    assert result == {"yellow_trips": 100, "yellow_trips_quarantine": 7}
    assert [d for _, d, _ in client.loads] == [
        "p.curated.yellow_trips$201901",
        "p.curated.yellow_trips_quarantine$201901",
    ]
    assert client.loads[0][0] == "gs://lake/curated/yellow/year=2019/month=01/*.parquet"


def test_row_count_mismatch_fails_loudly(report):
    client = FakeClient({"yellow_trips": 99, "yellow_trips_quarantine": 7})
    with pytest.raises(load.LoadVerificationError, match="expected 100"):
        load.load_month(MONTH, root="gs://lake", project="p", client=client)


def test_empty_quarantine_clears_partition_instead_of_loading(report):
    report["quarantined_rows"] = 0
    client = FakeClient({"yellow_trips": 100})
    load.load_month(MONTH, root="gs://lake", project="p", client=client)
    assert client.deletes == ["p.curated.yellow_trips_quarantine$201901"]
    assert len(client.loads) == 1


def test_report_for_wrong_month_is_rejected(report):
    report["month"] = "2019-02"
    with pytest.raises(load.LoadVerificationError):
        load.load_month(MONTH, root="gs://lake", project="p", client=FakeClient({}))


def test_local_lake_root_is_rejected():
    with pytest.raises(ValueError, match="gs://"):
        load.load_month(MONTH, root="data/lake", project="p", client=FakeClient({}))
