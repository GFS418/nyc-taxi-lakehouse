"""Spark job: one raw TLC month -> canonical schema -> hard DQ rules -> curated + quarantine.

Idempotent: a run overwrites exactly its own month's directories. The DQ report is the commit
marker. It is deleted before any output is written, and re-written only after the written
output has been read back and reconciled against the source row count. The warehouse load
refuses to run without it, so a crashed run can never reach BigQuery.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import pyspark
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.config import (
    Month,
    curated_dir_uri,
    dq_report_uri,
    lake_root,
    quarantine_dir_uri,
    raw_file_uri,
)
from lakehouse.quality import classify
from lakehouse.schema import QUARANTINE_COLUMNS, TRIP_COLUMNS, canonicalize
from lakehouse.spark import build_spark
from lakehouse.storage import delete_if_exists, write_json

log = logging.getLogger(__name__)


class ReconciliationError(RuntimeError):
    """Written output does not account for every source row exactly once."""


def spark_path(uri: str) -> str:
    return uri if "://" in uri else f"file://{os.path.abspath(uri)}"


def run_month(
    spark: SparkSession, month: Month, root: str, *, processed_at: datetime | None = None
) -> dict[str, Any]:
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise RuntimeError("the Spark session time zone must be UTC")
    report_uri = dq_report_uri(root, month)
    delete_if_exists(report_uri)

    raw_uri = raw_file_uri(root, month)
    raw = spark.read.parquet(spark_path(raw_uri))
    source_rows = raw.count()
    processed_at = processed_at or datetime.now(UTC)
    canonical = canonicalize(raw, source_month=month.first_day, processed_at=processed_at)
    classified = classify(canonical, month).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        primary = {
            r["reject_reason"]: r["count"]
            for r in classified.groupBy("reject_reason").count().collect()
        }
        rule_hits = {
            r["reason"]: r["count"]
            for r in classified.select(F.explode("reject_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        }
        null_passengers = classified.filter(F.col("passenger_count").isNull()).count()
        curated_out, quarantine_out = (
            spark_path(curated_dir_uri(root, month)),
            spark_path(quarantine_dir_uri(root, month)),
        )
        classified.filter(F.col("reject_reason").isNull()).select(*TRIP_COLUMNS).write.mode(
            "overwrite"
        ).parquet(curated_out)
        classified.filter(F.col("reject_reason").isNotNull()).select(
            *QUARANTINE_COLUMNS
        ).write.mode("overwrite").parquet(quarantine_out)
    finally:
        classified.unpersist()

    curated_rows = primary.pop(None, 0)
    quarantined_rows = sum(primary.values())
    written_curated = spark.read.parquet(curated_out).count()
    written_quarantine = spark.read.parquet(quarantine_out).count()
    if not (
        written_curated == curated_rows
        and written_quarantine == quarantined_rows
        and curated_rows + quarantined_rows == source_rows
    ):
        raise ReconciliationError(
            f"{month}: source={source_rows} curated={curated_rows}/{written_curated} "
            f"quarantined={quarantined_rows}/{written_quarantine}"
        )

    report = {
        "month": str(month),
        "source_file": raw_uri,
        "source_rows": source_rows,
        "curated_rows": curated_rows,
        "quarantined_rows": quarantined_rows,
        "quarantine_rate": round(quarantined_rows / source_rows, 6) if source_rows else 0.0,
        "reject_reason_counts": dict(sorted(primary.items())),
        "rule_hit_counts": dict(sorted(rule_hits.items())),
        "passenger_count_null_rows": null_passengers,
        "processed_at": processed_at.isoformat(),
        "spark_version": pyspark.__version__,
    }
    write_json(report, report_uri)
    log.info("%s: %s curated, %s quarantined", month, curated_rows, quarantined_rows)
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--month", required=True, type=Month.parse, help="YYYY-MM")
    parser.add_argument("--lake-root", default=lake_root())
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark("transform", gcs=args.lake_root.startswith("gs://"))
    try:
        print(json.dumps(run_month(spark, args.month, args.lake_root), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
