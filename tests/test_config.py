import pytest

from lakehouse.config import Month, curated_dir_uri, raw_file_uri


def test_month_parse_and_format_round_trip():
    assert str(Month.parse("2019-01")) == "2019-01"


@pytest.mark.parametrize("bad", ["2019-1", "201901", "2019-13", "2019-00"])
def test_month_rejects_bad_input(bad):
    with pytest.raises(ValueError):
        Month.parse(bad)


def test_month_next_rolls_over_year():
    assert Month(2019, 12).next() == Month(2020, 1)
    assert Month(2019, 5).next() == Month(2019, 6)


def test_partition_id_is_yyyymm():
    assert Month(2024, 6).partition_id == "202406"


def test_lake_layout_is_hive_partitioned_and_deterministic():
    m = Month(2023, 1)
    assert raw_file_uri("gs://b", m) == (
        "gs://b/raw/yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet"
    )
    assert curated_dir_uri("gs://b", m) == "gs://b/curated/yellow/year=2023/month=01"
