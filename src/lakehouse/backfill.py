"""Run many months through the whole pipeline with a single Spark session.

Resumable by design: a month whose data-quality report agrees with its BigQuery partition row
counts is skipped unless --force is passed. Nothing about a month's processing changes here, so
re-running one still replaces it rather than appending to it.

Months TLC has not published yet are reported and skipped, not treated as failures. Any other
failure is recorded and the run continues, unless --stop-on-error is given.

Usage: uv run python -m lakehouse.backfill --start 2019-01 --end 2023-12
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from lakehouse.config import Month, dq_report_uri, lake_root
from lakehouse.ingest import SourceNotPublishedError, ingest_month
from lakehouse.load import load_month
from lakehouse.spark import build_spark
from lakehouse.storage import exists, read_json
from lakehouse.transform import run_month

log = logging.getLogger("backfill")
TABLES = ("yellow_trips", "yellow_trips_quarantine")


def month_range(start: Month, end: Month) -> list[Month]:
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    months, month = [], start
    while month <= end:
        months.append(month)
        month = month.next()
    return months


def loaded_partitions(client: Any, project: str, dataset: str) -> dict[str, dict[str, int]]:
    """Row counts already in BigQuery, per partition, in one metadata query."""
    sql = f"""
        SELECT table_name, partition_id, total_rows
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name IN {TABLES}
    """
    partitions: dict[str, dict[str, int]] = defaultdict(dict)
    for row in client.query(sql).result():
        partitions[row["partition_id"]][row["table_name"]] = row["total_rows"]
    return partitions


def already_loaded(month: Month, root: str, partitions: dict[str, dict[str, int]]) -> bool:
    """True when the lake's report and both warehouse partitions agree for this month."""
    if not exists(dq_report_uri(root, month)):
        return False
    report = read_json(dq_report_uri(root, month))
    counts = partitions.get(month.partition_id, {})
    return (
        counts.get("yellow_trips") == report["curated_rows"]
        # A month with nothing quarantined has no partition at all, which is a match for 0.
        and counts.get("yellow_trips_quarantine", 0) == report["quarantined_rows"]
    )


def run_backfill(
    months: list[Month],
    *,
    root: str,
    project: str,
    dataset: str = "curated",
    force: bool = False,
    stop_on_error: bool = False,
    client: Any = None,
) -> dict[str, Any]:
    client = client or bigquery.Client(project=project)
    partitions = {} if force else loaded_partitions(client, project, dataset)
    spark = build_spark("backfill", gcs=root.startswith("gs://"))
    started = datetime.now(UTC)
    results: list[dict[str, Any]] = []
    try:
        for index, month in enumerate(months, start=1):
            prefix = f"[{index}/{len(months)}] {month}"
            if not force and already_loaded(month, root, partitions):
                log.info("%s already loaded, skipping", prefix)
                results.append({"month": str(month), "status": "skipped"})
                continue
            clock = time.monotonic()
            try:
                ingest_month(month, root)
                report = run_month(spark, month, root)
                load_month(month, root=root, project=project, dataset=dataset, client=client)
            except SourceNotPublishedError:
                log.warning("%s not published by TLC yet", prefix)
                results.append({"month": str(month), "status": "not_published"})
                continue
            except Exception as exc:  # noqa: BLE001 - recorded, and re-raised only on request
                log.error("%s failed: %s", prefix, exc)
                results.append({"month": str(month), "status": "failed", "error": str(exc)})
                if stop_on_error:
                    raise
                continue
            results.append(
                {
                    "month": str(month),
                    "status": "ok",
                    "source_rows": report["source_rows"],
                    "curated_rows": report["curated_rows"],
                    "quarantined_rows": report["quarantined_rows"],
                    "quarantine_rate": report["quarantine_rate"],
                    "reject_reason_counts": report["reject_reason_counts"],
                    "seconds": round(time.monotonic() - clock, 1),
                }
            )
            log.info(
                "%s done in %ss: %s curated, %s quarantined",
                prefix,
                results[-1]["seconds"],
                f"{report['curated_rows']:,}",
                f"{report['quarantined_rows']:,}",
            )
    finally:
        spark.stop()

    done = [r for r in results if r["status"] == "ok"]
    reasons: Counter[str] = Counter()
    for result in done:
        reasons.update(result["reject_reason_counts"])
    source_rows = sum(r["source_rows"] for r in done)
    quarantined = sum(r["quarantined_rows"] for r in done)
    return {
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "elapsed_minutes": round((datetime.now(UTC) - started).total_seconds() / 60, 1),
        "months_requested": len(months),
        "months_processed": len(done),
        "months_skipped": sum(1 for r in results if r["status"] == "skipped"),
        "months_failed": sum(1 for r in results if r["status"] == "failed"),
        "source_rows": source_rows,
        "curated_rows": sum(r["curated_rows"] for r in done),
        "quarantined_rows": quarantined,
        "quarantine_rate": round(quarantined / source_rows, 6) if source_rows else 0.0,
        "reject_reason_counts": dict(sorted(reasons.items())),
        "months": results,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", required=True, type=Month.parse, help="YYYY-MM")
    parser.add_argument("--end", required=True, type=Month.parse, help="YYYY-MM, inclusive")
    parser.add_argument("--lake-root", default=lake_root())
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--dataset", default="curated")
    parser.add_argument("--force", action="store_true", help="reprocess months already loaded")
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--summary", default="docs/backfill_summary.json")
    args = parser.parse_args(argv)
    if not args.project:
        parser.error("--project or GCP_PROJECT_ID is required")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    summary = run_backfill(
        month_range(args.start, args.end),
        root=args.lake_root,
        project=args.project,
        dataset=args.dataset,
        force=args.force,
        stop_on_error=args.stop_on_error,
    )
    Path(args.summary).write_text(json.dumps(summary, indent=2) + "\n")
    log.info(
        "%s months, %s rows in, %s curated, %s quarantined (%.3f%%), %s minutes",
        summary["months_processed"],
        f"{summary['source_rows']:,}",
        f"{summary['curated_rows']:,}",
        f"{summary['quarantined_rows']:,}",
        100 * summary["quarantine_rate"],
        summary["elapsed_minutes"],
    )
    if summary["months_failed"]:
        raise SystemExit(f"{summary['months_failed']} month(s) failed; see {args.summary}")


if __name__ == "__main__":
    main()
