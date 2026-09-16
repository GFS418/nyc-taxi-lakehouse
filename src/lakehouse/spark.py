"""One SparkSession recipe for the job, the tests, and (later) Airflow."""

from __future__ import annotations

import os
from pathlib import Path

import requests
from pyspark.sql import SparkSession

# 4.x is built against Hadoop 3.4, the version Spark 4.1 bundles. The 3.x connector calls a
# VectoredReadUtils method whose signature changed there, so Parquet reads from gs:// fail
# with NoSuchMethodError on any file large enough to use vectored IO.
GCS_CONNECTOR_VERSION = "4.0.5"
GCS_CONNECTOR_URL = (
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/"
    f"{GCS_CONNECTOR_VERSION}/gcs-connector-{GCS_CONNECTOR_VERSION}-shaded.jar"
)
JAR_CACHE = Path(
    os.environ.get("LAKEHOUSE_JAR_CACHE", Path.home() / ".cache" / "nyc-taxi-lakehouse")
)


def gcs_connector_jar() -> Path:
    """Download the shaded GCS connector once. It must be on the driver classpath at JVM start."""
    jar = JAR_CACHE / f"gcs-connector-{GCS_CONNECTOR_VERSION}-shaded.jar"
    if not jar.exists():
        jar.parent.mkdir(parents=True, exist_ok=True)
        tmp = jar.with_suffix(".tmp")
        with requests.get(GCS_CONNECTOR_URL, stream=True, timeout=(10, 300)) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as out:
                for chunk in resp.iter_content(1 << 20):
                    out.write(chunk)
        tmp.rename(jar)
    return jar


def build_spark(app_name: str = "nyc-taxi-lakehouse", *, gcs: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName(app_name)
        # Source timestamps are timezone-naive; a UTC session makes every cast a no-op.
        .config("spark.sql.session.timeZone", "UTC")
        # Spark defaults TIMESTAMP columns to deprecated INT96; write standard INT64 micros.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.ui.enabled", os.environ.get("SPARK_UI_ENABLED", "false"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if gcs:
        jar = str(gcs_connector_jar())
        builder = (
            builder.config("spark.jars", jar)
            .config("spark.driver.extraClassPath", jar)
            .config(
                "spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
            )
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            )
            .config(
                "spark.hadoop.fs.gs.auth.type",
                os.environ.get("GCS_AUTH_TYPE", "APPLICATION_DEFAULT"),
            )
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return session
