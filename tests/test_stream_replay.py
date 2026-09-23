import json
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakehouse import stream_replay as sr
from lakehouse.config import Month, raw_file_uri

PICKUP = datetime(2024, 6, 14, 17, 5)
ROW = {
    "vendorid": 2,
    "tpep_pickup_datetime": PICKUP,
    "tpep_dropoff_datetime": PICKUP + timedelta(minutes=15),
    "passenger_count": 1,
    "trip_distance": 3.2,
    "ratecodeid": 1,
    "store_and_fwd_flag": "N",
    "pulocationid": 161,
    "dolocationid": 236,
    "payment_type": 1,
    "fare_amount": 15.5,
    "extra": 2.5,
    "mta_tax": 0.5,
    "tip_amount": 3.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 1.0,
    "total_amount": 22.5,
    "congestion_surcharge": 2.5,
    "airport_fee": 0.0,
}
ENDED = datetime(2026, 9, 22, 18, 0)
EMITTED = datetime(2026, 9, 22, 22, 0, tzinfo=UTC)
FIXED = dict(now_local=lambda: ENDED, now_utc=lambda: EMITTED)


def event(**overrides):
    return sr.build_event({**ROW, **overrides}, ended_at=ENDED, emitted_at=EMITTED)


def test_event_ends_now_and_keeps_its_real_duration():
    e = event()
    assert e["dropoff_datetime"] == "2026-09-22 18:00:00"
    assert e["pickup_datetime"] == "2026-09-22 17:45:00"


def test_money_is_exact_decimal_text():
    e = event(fare_amount=0.1 + 0.2)
    assert e["fare_amount"] == "0.30"
    assert e["cbd_congestion_fee"] is None  # the column does not exist before 2025


def test_valid_event_passes_the_avro_schema():
    assert sr.is_valid(event())


def test_encoding_names_union_branches_as_pubsub_requires():
    wire = json.loads(sr.encode(event()))
    assert wire["passenger_count"] == {"long": 1}
    assert wire["store_and_fwd_flag"] == {"string": "N"}
    assert wire["cbd_congestion_fee"] is None
    assert wire["fare_amount"] == "15.50"  # not a union, so not wrapped


def test_unrecorded_passenger_count_stays_null():
    e = event(passenger_count=None)
    assert e["passenger_count"] is None
    assert json.loads(sr.encode(e))["passenger_count"] is None


@pytest.mark.parametrize(
    "fault, broken",
    [
        ("negative_fare", lambda e: e["fare_amount"] == "-15.50" and e["total_amount"] == "-22.50"),
        ("invalid_zone", lambda e: e["pu_location_id"] == 0),
        ("dropoff_before_pickup", lambda e: e["dropoff_datetime"] < e["pickup_datetime"]),
        ("impossible_speed", lambda e: e["trip_distance"] == 250.0),
    ],
)
def test_warehouse_faults_break_one_thing_and_stay_schema_valid(fault, broken):
    bad = sr.FAULT_FNS[fault](event())
    assert broken(bad)
    assert sr.is_valid(bad)  # must reach the warehouse, where the data-quality rules catch it


def test_negating_a_zero_fare_still_produces_a_negative_one():
    assert sr.FAULT_FNS["negative_fare"](event(fare_amount=0.0))["fare_amount"] == "-10.00"


def test_schema_violation_fails_the_schema():
    assert not sr.is_valid(sr.FAULT_FNS["schema_violation"](event()))


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


def rows_ending_at(minutes):
    return [
        {
            **ROW,
            "tpep_pickup_datetime": PICKUP + timedelta(minutes=m),
            "tpep_dropoff_datetime": PICKUP + timedelta(minutes=m + 15),
        }
        for m in minutes
    ]


def test_replay_compresses_time_by_the_speedup():
    clock, sent = FakeClock(), []
    stats = sr.replay(
        rows_ending_at([0, 10, 20]),
        speedup=60,
        fault_rate=0,
        seed=1,
        publish=sent.append,
        publish_single=lambda data: None,
        clock=clock.now,
        sleep=clock.sleep,
        **FIXED,
    )
    assert stats["published"] == len(sent) == 3
    assert clock.t == pytest.approx(20)  # twenty minutes of trips at 60x takes twenty seconds


def run_with_faults(seed):
    sent, rejected = [], []

    def single(data):
        rejected.append(data)
        raise ValueError("schema")

    stats = sr.replay(
        rows_ending_at([0] * 300),
        speedup=1,
        fault_rate=0.5,
        seed=seed,
        publish=sent.append,
        publish_single=single,
        clock=lambda: 0.0,
        sleep=lambda s: None,
        **FIXED,
    )
    return stats, sent, rejected


def test_faults_are_reproducible_from_the_seed():
    assert run_with_faults(7)[0] == run_with_faults(7)[0]


def test_duplicates_resend_the_same_event_and_violations_never_publish():
    stats, sent, rejected = run_with_faults(7)
    assert stats["rejected_at_publish"] == stats["injected_schema_violation"] == len(rejected) > 0
    ids = Counter(json.loads(m)["event_id"] for m in sent)
    assert sum(1 for n in ids.values() if n == 2) == stats["injected_duplicate"] > 0
    assert stats["published"] == len(sent)


def test_load_trips_keeps_the_window_in_dropoff_order(tmp_path):
    root = str(tmp_path)
    dropoffs = [datetime(2024, 6, 14, 17, m) for m in (25, 0, 10)] + [datetime(2024, 6, 14, 16, 59)]
    table = pa.table(
        {
            "VendorID": [1, 2, 3, 4],
            "tpep_pickup_datetime": pa.array(
                [d - timedelta(minutes=5) for d in dropoffs], pa.timestamp("us")
            ),
            "tpep_dropoff_datetime": pa.array(dropoffs, pa.timestamp("us")),
        }
    )
    path = Path(raw_file_uri(root, Month(2024, 6)))
    path.parent.mkdir(parents=True)
    pq.write_table(table, path)
    rows = sr.load_trips(Month(2024, 6), date(2024, 6, 14), time(17, 0), 20, root)
    assert [r["vendorid"] for r in rows] == [2, 3]  # 17:00 and 17:10; 16:59 and 17:25 fall outside
