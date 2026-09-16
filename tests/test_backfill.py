import pytest

from lakehouse import backfill
from lakehouse.config import Month


def test_month_range_is_inclusive_and_rolls_over_years():
    months = backfill.month_range(Month(2019, 11), Month(2020, 2))
    assert [str(m) for m in months] == ["2019-11", "2019-12", "2020-01", "2020-02"]


def test_month_range_rejects_backwards_range():
    with pytest.raises(ValueError, match="before start"):
        backfill.month_range(Month(2020, 1), Month(2019, 12))


@pytest.fixture
def report(monkeypatch):
    data = {"curated_rows": 100, "quarantined_rows": 7}
    monkeypatch.setattr(backfill, "exists", lambda uri: True)
    monkeypatch.setattr(backfill, "read_json", lambda uri: data)
    return data


def test_month_counts_matching_both_partitions_is_skipped(report):
    partitions = {"201901": {"yellow_trips": 100, "yellow_trips_quarantine": 7}}
    assert backfill.already_loaded(Month(2019, 1), "gs://lake", partitions) is True


@pytest.mark.parametrize(
    "partitions",
    [
        {},  # never loaded
        {"201901": {"yellow_trips": 99, "yellow_trips_quarantine": 7}},  # partial curated load
        {"201901": {"yellow_trips": 100}},  # quarantine never loaded
    ],
)
def test_month_is_reprocessed_when_warehouse_disagrees(report, partitions):
    assert backfill.already_loaded(Month(2019, 1), "gs://lake", partitions) is False


def test_month_with_no_quarantine_rows_needs_no_partition(report):
    report["quarantined_rows"] = 0
    partitions = {"201901": {"yellow_trips": 100}}
    assert backfill.already_loaded(Month(2019, 1), "gs://lake", partitions) is True


def test_missing_report_means_not_loaded(monkeypatch):
    monkeypatch.setattr(backfill, "exists", lambda uri: False)
    assert backfill.already_loaded(Month(2019, 1), "gs://lake", {"201901": {}}) is False
