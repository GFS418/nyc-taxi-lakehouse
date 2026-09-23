"""Replay real TLC trips as a live Pub/Sub feed.

Each trip is emitted at the moment its meter would disengage, re-stamped so it ends "now" in NYC
wall-clock time while keeping its real duration. Time is compressed by --speedup, so a busy twenty
minutes replays in one. A seeded fraction of events can be corrupted on purpose, to exercise the
stream's data-quality handling end to end, including one fault the topic's schema must reject.

Usage:
  uv run python -m lakehouse.stream_replay --month 2024-06 --day 2024-06-14 --start 17:00 \\
      --minutes 20 --speedup 20 --fault-rate 0.02
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
import uuid
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import fastavro
import fastavro.validation
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from lakehouse.config import Month, lake_root, raw_file_uri
from lakehouse.storage import filesystem_for

log = logging.getLogger(__name__)

NYC = ZoneInfo("America/New_York")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
WALL_CLOCK = "%Y-%m-%d %H:%M:%S"
AVSC_PATH = Path(__file__).resolve().parents[2] / "schemas" / "stream" / "trip_event.avsc"
SCHEMA = fastavro.parse_schema(json.loads(AVSC_PATH.read_text()))

MONEY = (
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
)
NULLABLE_MONEY = ("congestion_surcharge", "airport_fee", "cbd_congestion_fee")
# Avro's JSON encoding names the branch of every non-null union value, and Pub/Sub enforces it:
# {"passenger_count": {"long": 1}} is valid, {"passenger_count": 1} is rejected.
UNION_BRANCH = {
    "passenger_count": "long",
    "ratecode_id": "long",
    "store_and_fwd_flag": "string",
    **{m: "string" for m in NULLABLE_MONEY},
}
FAULTS = (
    "negative_fare",
    "invalid_zone",
    "dropoff_before_pickup",
    "impossible_speed",
    "duplicate",
    "schema_violation",
)


def money(value: float | None) -> str | None:
    """Exact two-place decimal text: 0.1 + 0.2 becomes "0.30", not "0.30000000000000004"."""
    return None if value is None else str(Decimal(repr(value)).quantize(Decimal("0.01")))


def _int(value: Any) -> int | None:
    return None if value is None else int(value)


def load_trips(
    month: Month, day: date, start: clock_time, minutes: int, root: str
) -> list[dict[str, Any]]:
    """Raw trips from the lake whose meter disengaged inside the window, earliest first."""
    fs, path = filesystem_for(raw_file_uri(root, month))
    with fs.open_input_file(path) as f:
        table = pq.read_table(f)
    table = table.rename_columns([c.lower() for c in table.column_names])
    lo = datetime.combine(day, start)
    hi = lo + timedelta(minutes=minutes)
    dropoff = table["tpep_dropoff_datetime"]
    in_window = pc.and_(
        pc.greater_equal(dropoff, pa.scalar(lo, dropoff.type)),
        pc.less(dropoff, pa.scalar(hi, dropoff.type)),
    )
    return table.filter(in_window).sort_by("tpep_dropoff_datetime").to_pylist()


def build_event(row: dict[str, Any], *, ended_at: datetime, emitted_at: datetime) -> dict[str, Any]:
    """A trip as a plain Avro record, ending at `ended_at` (NYC wall clock), real duration kept."""
    duration = row["tpep_dropoff_datetime"] - row["tpep_pickup_datetime"]
    dropoff = ended_at.replace(microsecond=0)
    pickup = dropoff - duration
    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": (emitted_at - EPOCH) // timedelta(microseconds=1),
        "vendor_id": int(row["vendorid"]),
        "pickup_datetime": pickup.strftime(WALL_CLOCK),
        "dropoff_datetime": dropoff.strftime(WALL_CLOCK),
        "passenger_count": _int(row.get("passenger_count")),
        "trip_distance": float(row["trip_distance"]),
        "ratecode_id": _int(row.get("ratecodeid")),
        "store_and_fwd_flag": row.get("store_and_fwd_flag"),
        "pu_location_id": int(row["pulocationid"]),
        "do_location_id": int(row["dolocationid"]),
        "payment_type": int(row["payment_type"]),
        **{m: money(row[m]) for m in MONEY},
        **{m: money(row.get(m)) for m in NULLABLE_MONEY},
    }


def is_valid(event: dict[str, Any]) -> bool:
    return fastavro.validation.validate(event, SCHEMA, raise_errors=False)


def encode(event: dict[str, Any]) -> bytes:
    """Avro JSON encoding: the exact form the topic's schema validates."""
    wire = {
        k: ({UNION_BRANCH[k]: v} if k in UNION_BRANCH and v is not None else v)
        for k, v in event.items()
    }
    return json.dumps(wire, separators=(",", ":")).encode()


def _negative_fare(e: dict[str, Any]) -> dict[str, Any]:
    bad = {**e, **{m: str(-Decimal(e[m])) for m in MONEY}}
    if Decimal(e["fare_amount"]) == 0:  # negating zero would not be negative
        bad["fare_amount"] = "-10.00"
    return bad


def _dropoff_before_pickup(e: dict[str, Any]) -> dict[str, Any]:
    pickup = datetime.strptime(e["pickup_datetime"], WALL_CLOCK)
    return {**e, "dropoff_datetime": (pickup - timedelta(minutes=1)).strftime(WALL_CLOCK)}


# Every fault but the last stays schema-valid on purpose: those must reach the warehouse and be
# caught by the data-quality rules there. The schema violation must never get that far.
FAULT_FNS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "negative_fare": _negative_fare,
    "invalid_zone": lambda e: {**e, "pu_location_id": 0},
    "dropoff_before_pickup": _dropoff_before_pickup,
    "impossible_speed": lambda e: {**e, "trip_distance": 250.0},
    "schema_violation": lambda e: {k: v for k, v in e.items() if k != "fare_amount"},
}


def replay(
    rows: list[dict[str, Any]],
    *,
    speedup: float,
    fault_rate: float,
    seed: int,
    publish: Callable[[bytes], Any],
    publish_single: Callable[[bytes], Any],
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    now_local: Callable[[], datetime] = lambda: datetime.now(NYC).replace(tzinfo=None),
    now_utc: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Counter[str]:
    """Emit each trip on its compressed schedule. Returns counts of what was sent and injected."""
    stats: Counter[str] = Counter()
    if not rows:
        return stats
    rng = random.Random(seed)
    first = rows[0]["tpep_dropoff_datetime"]
    started = clock()
    for row in rows:
        due = (row["tpep_dropoff_datetime"] - first).total_seconds() / speedup
        wait = due - (clock() - started)
        if wait > 0:
            sleep(wait)
        event = build_event(row, ended_at=now_local(), emitted_at=now_utc())
        fault = rng.choice(FAULTS) if rng.random() < fault_rate else None
        if fault:
            stats[f"injected_{fault}"] += 1
        if fault == "schema_violation":
            try:
                publish_single(encode(FAULT_FNS[fault](event)))
                stats["schema_violation_accepted"] += 1
            except Exception:  # noqa: BLE001 - rejection is the expected, correct outcome
                stats["rejected_at_publish"] += 1
            continue
        if fault and fault != "duplicate":
            event = FAULT_FNS[fault](event)
        payload = encode(event)
        publish(payload)
        stats["published"] += 1
        if fault == "duplicate":  # a producer retry: same event, delivered twice
            publish(payload)
            stats["published"] += 1
    return stats


def pubsub_publishers(project: str, topic: str):
    """A batching publisher for normal events and a one-message publisher for the fault.

    A publish request that contains one schema-invalid message fails as a whole, so the deliberate
    violation travels alone and cannot take valid events down with it.
    """
    from google.cloud import pubsub_v1

    batched = pubsub_v1.PublisherClient()
    single = pubsub_v1.PublisherClient(batch_settings=pubsub_v1.types.BatchSettings(max_messages=1))
    path = batched.topic_path(project, topic)
    futures = []

    def publish(data: bytes) -> None:
        futures.append(batched.publish(path, data))

    def publish_single(data: bytes) -> None:
        single.publish(path, data).result(timeout=30)

    def flush() -> int:
        failures = 0
        for future in futures:
            try:
                future.result(timeout=120)
            except Exception:  # noqa: BLE001 - counted and reported
                failures += 1
        return failures

    return publish, publish_single, flush


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--month", required=True, type=Month.parse, help="YYYY-MM of the source file"
    )
    parser.add_argument(
        "--day", required=True, type=date.fromisoformat, help="YYYY-MM-DD to replay"
    )
    parser.add_argument("--start", default="17:00", type=clock_time.fromisoformat)
    parser.add_argument("--minutes", type=int, default=20)
    parser.add_argument("--speedup", type=float, default=20.0)
    parser.add_argument("--fault-rate", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--lake-root", default=lake_root())
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--topic", default="trip-events")
    parser.add_argument("--dry-run", action="store_true", help="validate locally, publish nothing")
    args = parser.parse_args(argv)
    if (args.day.year, args.day.month) != (args.month.year, args.month.month):
        parser.error(f"--day {args.day} is not in --month {args.month}")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    rows = load_trips(args.month, args.day, args.start, args.minutes, args.lake_root)
    log.info("%s trips in the window; replaying at %sx", len(rows), args.speedup)
    common = dict(speedup=args.speedup, fault_rate=args.fault_rate, seed=args.seed)

    if args.dry_run:
        sent: list[bytes] = []

        def reject_locally(data: bytes) -> None:
            raise ValueError("schema violation")

        stats = replay(
            rows, publish=sent.append, publish_single=reject_locally, sleep=lambda s: None, **common
        )
        if sent:
            print(sent[0].decode())
    else:
        if not args.project:
            parser.error("--project or GCP_PROJECT_ID is required")
        publish, publish_single, flush = pubsub_publishers(args.project, args.topic)
        stats = replay(rows, publish=publish, publish_single=publish_single, **common)
        stats["publish_failures"] = flush()
    print(json.dumps(dict(sorted(stats.items())), indent=2))


if __name__ == "__main__":
    main()
