"""One DAG run per TLC month: wait for publication, ingest, transform, load, then model.

The run's data interval names the month, so a run touches exactly one month and nothing else.
Every task is idempotent, so re-running a month replaces it instead of appending to it, and a
failed run can simply be cleared and retried.

TLC publishes with a lag of roughly two months, and its CDN answers HTTP 403 until a file exists.
Rather than hard-coding that lag, the first task waits for the file to appear, in reschedule mode
so it occupies no worker while waiting.
"""

from __future__ import annotations

import logging

import pendulum
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import dag, task

from lakehouse.config import Month, lake_root, source_url

log = logging.getLogger(__name__)

# dbt writes into the image, not the read-only mounted repo.
DBT_BUILD = (
    "/opt/dbt-venv/bin/dbt build"
    " --project-dir /opt/project/dbt"
    " --profiles-dir /opt/project/dbt"
    " --target-path /tmp/dbt-target"
    " --log-path /tmp/dbt-logs"
    # The run tells dbt which month it owns, so incremental models rebuild exactly that
    # partition instead of inferring a window from what is already in the table.
    """ --vars '{"month": "{{ data_interval_start.strftime('%Y-%m') }}"}'"""
)


def month_of(context) -> Month:
    """The month this run owns, taken from its data interval rather than the wall clock."""
    start = context["data_interval_start"]
    return Month(start.year, start.month)


def source_is_published(**context) -> bool:
    month = month_of(context)
    status = requests.head(source_url(month), timeout=30).status_code
    log.info("TLC returned HTTP %s for %s", status, month)
    return status == 200


@dag(
    dag_id="nyc_taxi_monthly",
    description="Ingest, clean, load and model one month of NYC yellow taxi trips.",
    schedule="@monthly",
    # 2019-01 through 2024-06 were loaded by scripts/backfill.py. This DAG carries on from there.
    start_date=pendulum.datetime(2024, 7, 1, tz="UTC"),
    # Bounded to the agreed scope. Remove end_date to keep following TLC to the present.
    end_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["nyc-taxi", "lakehouse"],
)
def nyc_taxi_monthly():
    wait_for_publication = PythonSensor(
        task_id="wait_for_tlc_publication",
        python_callable=source_is_published,
        mode="reschedule",  # frees the worker slot between checks
        poke_interval=6 * 60 * 60,
        timeout=75 * 24 * 60 * 60,
    )

    @task
    def ingest(**context) -> dict:
        from lakehouse.ingest import ingest_month

        month = month_of(context)
        manifest = ingest_month(month, lake_root())
        log.info("%s: %s rows landed at %s", month, manifest["num_rows"], manifest["raw_uri"])
        return {"month": str(month), "rows": manifest["num_rows"]}

    @task
    def transform(**context) -> dict:
        from lakehouse.spark import build_spark
        from lakehouse.transform import run_month

        month = month_of(context)
        root = lake_root()
        spark = build_spark("airflow-transform", gcs=root.startswith("gs://"))
        try:
            report = run_month(spark, month, root)
        finally:
            spark.stop()
        log.info(
            "%s: %s curated, %s quarantined (%.3f%%)",
            month,
            report["curated_rows"],
            report["quarantined_rows"],
            100 * report["quarantine_rate"],
        )
        return {k: report[k] for k in ("month", "curated_rows", "quarantined_rows")}

    @task
    def load(**context) -> dict:
        import os

        from lakehouse.load import load_month

        month = month_of(context)
        loaded = load_month(month, root=lake_root(), project=os.environ["GCP_PROJECT_ID"])
        log.info("%s: loaded %s", month, loaded)
        return loaded

    build_models = BashOperator(task_id="dbt_build", bash_command=DBT_BUILD)

    wait_for_publication >> ingest() >> transform() >> load() >> build_models


nyc_taxi_monthly()
