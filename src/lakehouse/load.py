"""Load one curated month from the lake into BigQuery, replacing exactly that month.

Each table is written through a partition decorator (table$YYYYMM) with WRITE_TRUNCATE, so
BigQuery swaps in the new partition atomically and leaves every other month untouched. A
re-run replaces the month instead of appending to it. Each load then checks its output row
count against the Spark job's data-quality report and fails loudly on any mismatch.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from lakehouse.config import Month, curated_dir_uri, dq_report_uri, lake_root, quarantine_dir_uri
from lakehouse.storage import read_json

log = logging.getLogger(__name__)
SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas" / "bigquery"


class LoadVerificationError(RuntimeError):
    pass


@dataclass(frozen=True)
class TableSpec:
    table: str
    schema_file: str
    partition_field: str
    clustering: tuple[str, ...]
    dir_uri: Callable[[str, Month], str]
    report_key: str


TABLES = (
    TableSpec(
        "yellow_trips",
        "yellow_trips.json",
        "pickup_datetime",
        ("pu_location_id", "do_location_id"),
        curated_dir_uri,
        "curated_rows",
    ),
    TableSpec(
        "yellow_trips_quarantine",
        "yellow_trips_quarantine.json",
        "source_month",
        ("reject_reason",),
        quarantine_dir_uri,
        "quarantined_rows",
    ),
)


def load_schema(schema_file: str) -> list[bigquery.SchemaField]:
    fields = json.loads((SCHEMA_DIR / schema_file).read_text())
    return [bigquery.SchemaField.from_api_repr(f) for f in fields]


def job_config(spec: TableSpec) -> bigquery.LoadJobConfig:
    parquet = bigquery.ParquetOptions()
    parquet.enable_list_inference = True  # reject_reasons arrives as REPEATED STRING
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=load_schema(spec.schema_file),
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field=spec.partition_field
        ),
        clustering_fields=list(spec.clustering),
        # No decimal_target_types here: BigQuery rejects a load that sets both that and an
        # explicit schema. The schema already types every money column as NUMERIC.
        parquet_options=parquet,
    )


def destination(project: str, dataset: str, spec: TableSpec, month: Month) -> str:
    return f"{project}.{dataset}.{spec.table}${month.partition_id}"


def load_month(
    month: Month, *, root: str, project: str, dataset: str = "curated", client: Any = None
) -> dict[str, int]:
    if not root.startswith("gs://"):
        raise ValueError("BigQuery load jobs read from Cloud Storage: use a gs:// lake root")
    report = read_json(dq_report_uri(root, month))
    if report.get("month") != str(month):
        raise LoadVerificationError(f"DQ report is for {report.get('month')}, not {month}")
    client = client or bigquery.Client(project=project)
    loaded: dict[str, int] = {}
    for spec in TABLES:
        dest = destination(project, dataset, spec, month)
        expected = int(report[spec.report_key])
        if expected == 0:
            # Nothing to load, but a previous run may have left rows: clear the partition.
            client.delete_table(dest, not_found_ok=True)
            loaded[spec.table] = 0
            continue
        job = client.load_table_from_uri(
            f"{spec.dir_uri(root, month)}/*.parquet", dest, job_config=job_config(spec)
        )
        job.result()
        if job.output_rows != expected:
            raise LoadVerificationError(
                f"{dest}: loaded {job.output_rows} rows but the DQ report expected {expected}"
            )
        loaded[spec.table] = job.output_rows
        log.info("loaded %s rows into %s", job.output_rows, dest)
    return loaded


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--month", required=True, type=Month.parse, help="YYYY-MM")
    parser.add_argument("--lake-root", default=lake_root())
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--dataset", default="curated")
    args = parser.parse_args(argv)
    if not args.project:
        parser.error("--project or GCP_PROJECT_ID is required")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = load_month(args.month, root=args.lake_root, project=args.project, dataset=args.dataset)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
