# Airflow with Spark in the same image: the DAG runs PySpark locally, the way the CLI does.
#
# dbt gets its own virtualenv. dbt and Airflow pin conflicting versions of shared libraries, and
# keeping them apart is cheaper than fighting a resolver on every rebuild.
FROM apache/airflow:3.3.1-python3.12

USER root

# PySpark 4 needs Java 17 or newer. headless is enough: nothing draws a window.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl \
    && rm -rf /var/lib/apt/lists/*

# Bake in the Cloud Storage connector so no DAG run pays for a 38 MB download.
RUN mkdir -p /opt/spark-jars \
    && curl -sSL -o /opt/spark-jars/gcs-connector-4.0.5-shaded.jar \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/4.0.5/gcs-connector-4.0.5-shaded.jar \
    && chown -R airflow /opt/spark-jars

RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-bigquery>=1.10,<1.12" \
    && chown -R airflow /opt/dbt-venv

USER airflow
RUN pip install --no-cache-dir \
    "pyspark>=4.0,<4.2" "pyarrow>=18" "google-cloud-bigquery>=3.25" "requests>=2.32"
