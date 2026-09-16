# Orchestration

One Airflow DAG, `nyc_taxi_monthly`, running the whole pipeline for a single month:

```
wait_for_tlc_publication -> ingest -> transform -> load -> dbt_build
```

## Why it is shaped this way

- **The data interval names the month.** A run reads its own interval, so it can never process
  "whatever is newest". Backfilling is just catchup, and re-running a month replaces it.
- **The sensor waits instead of assuming.** TLC publishes roughly two months late, on no fixed day,
  and its CDN answers HTTP 403 until the file exists. The sensor polls in reschedule mode, so a
  waiting run holds no worker slot.
- **Retries are safe.** Every task is idempotent: the lake key is a function of the month, Spark
  overwrites that month's directories, and the warehouse load replaces one partition.
- **dbt runs per month.** The fact is incremental, so a monthly build touches one partition rather
  than rescanning 219 million rows.

## Running it

```bash
cp .env.example .env     # GCP_PROJECT_ID and LAKE_ROOT
docker compose up --build
```

Then open http://localhost:8080. The generated admin password is inside the container:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

To run one month end to end without the scheduler, which is also how it is tested:

```bash
docker compose run --rm airflow bash -lc "airflow db migrate && airflow dags test nyc_taxi_monthly 2024-07-01"
```

## Credentials

The host's `~/.config/gcloud` is mounted read only, so the container uses the same Application
Default Credentials you use locally. No service-account key is created or copied. Terraform does
create a `lakehouse-pipeline` service account for a real deployment, deliberately without a key:
in production it would authenticate through Workload Identity instead.

## Scope

The DAG starts at 2024-07, where `scripts/backfill.py` finished, and stops at the end of 2024.
Remove `end_date` from the DAG to keep following TLC to the present.
