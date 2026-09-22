# NYC Taxi Lakehouse

An end-to-end lakehouse and ELT platform on NYC TLC yellow taxi trips: 256 million rows across
2019 to 2024. Raw Parquet lands in Google Cloud Storage, PySpark
enforces a schema contract and data-quality rules, BigQuery holds the warehouse, dbt models it
into a star schema, and Airflow runs it one month at a time.

The point is not the tool count. Every stage can be re-run without double-counting, every
rejected row is kept with its reasons, and every cost decision is written down.

## Architecture

```mermaid
flowchart LR
    TLC["TLC CDN<br/>monthly Parquet"] -->|"ingest<br/>validate, atomic write,<br/>manifest"| RAW[("GCS raw<br/>year=/month=")]
    RAW -->|"PySpark<br/>schema contract,<br/>hard DQ rules"| CUR[("GCS curated")]
    RAW --> Q[("GCS quarantine<br/>+ reasons")]
    CUR -->|"load: table$YYYYMM<br/>WRITE_TRUNCATE,<br/>row counts verified"| BQ[("BigQuery<br/>curated.yellow_trips")]
    Q --> BQQ[("BigQuery<br/>quarantine")]
    BQ -->|dbt| STG["staging"] --> MART["star schema<br/>and marts"] --> BI["Looker Studio"]
    AF{{"Airflow: one run per month"}} -.-> RAW
    TF{{"Terraform: bucket, datasets,<br/>tables, IAM, budget"}} -.-> BQ
```

## What the data-quality rules caught

The full 2019-2023 backfill, 60 months processed in 145 minutes with no failures. Every source row
is accounted for, and the job fails if it is not.

| Scope | Source rows | Curated | Quarantined |
|-------|------------:|--------:|------------:|
| 2019-2023 | 218,118,168 | 216,051,983 | 2,066,185 (0.95%) |

| Month | Source rows | Curated | Quarantined | Largest reason |
|-------|------------:|--------:|------------:|----------------|
| 2019-01 | 7,696,617 | 7,682,131 | 14,486 (0.19%) | Refund/void reversal pairs |
| 2024-06 | 3,539,193 | 3,434,764 | 104,429 (2.95%) | Refund/void reversal pairs |

Most negative fares are not junk. They exactly cancel an earlier charge for the same trip, so both
halves are quarantined and revenue nets to what was collected. The full rule set, thresholds, and
reasoning are in [docs/data_quality_rules.md](docs/data_quality_rules.md).

## What six years of trips show

| Year | Trips | Revenue | Avg fare | Tips as share of fares |
|------|------:|--------:|---------:|-----------------------:|
| 2019 | 84,256,744 | $1.62B | $19.20 | 16.4% |
| 2020 | 24,437,748 | $0.45B | $18.39 | 16.6% |
| 2021 | 30,600,762 | $0.61B | $19.80 | 17.4% |
| 2022 | 39,163,369 | $0.86B | $21.92 | 18.6% |
| 2023 | 37,593,360 | $1.09B | $29.01 | 18.0% |
| 2024 | 39,904,569 | $1.15B | $28.74 | 17.1% |

- **April 2020 was the floor.** 235,479 trips, against 7,452,418 in April 2019. A 97% collapse.
- **Ridership never came back, but revenue mostly did.** 2024 carried 47% of 2019's trips and took
  71% of its revenue, because the average fare rose by half.
- **Airports carry more of the business.** Airport pickups grew from 5.8% of all trips in 2019 to
  7.8% in 2024, recovering faster than street hails.
- **The business is Manhattan.** Manhattan-to-Manhattan trips are 84% of the six-year total.

The tip column counts tips against fares across all trips. The TLC records tips automatically for
card payments and never records cash tips, so treat it as a floor rather than a tipping rate.

## Dashboard

Five reporting views feed it, each denormalized so charts need no joins: daily overview, hourly
profile, borough flows, zone flows, and airport traffic.

<!-- Add the Looker Studio link here once the report is shared. -->

A public dashboard runs a query for every visitor, on the owner's bill. Four of the five views are
small on purpose (2,192 daily rows, 44,539 hourly, 2,355 borough pairs, 366 airport rows), so they
can be served from cached extracts at no cost. Only the 1.6 million-row zone detail queries live.

## Decisions worth reading

- **Idempotency by construction.** Lake keys are pure functions of the month. BigQuery loads swap
  exactly one month partition. The Spark job's DQ report is a commit marker, so a crashed run can't be loaded.
- **Schema drift is a contract.** Types change between files in 2023. A single canonical schema
  absorbs known variants, and any unknown column fails the month.
- **Time zones.** Source timestamps are NYC wall-clock with no zone, and every layer preserves that.
- **Cost.** Month partitions, physical storage billing, dropping a 16.7 GiB redundant column, and
  per-query byte caps in dbt.
- **CI that can actually fail the build.** Every pull request runs the tests and a real dbt build
  against BigQuery, authenticated without any stored key, in a dataset it creates and drops.
- **A star schema that can fail.** Dimensions come from seeds transcribed from the TLC dictionary,
  never from `select distinct` over the facts, so relationship tests catch a code the data invents.
  The fact is incremental by month, so a routine build scans one month, not 219 million rows.

All of it, with evidence, is in [docs/design.md](docs/design.md).

## Status

| Phase | Scope | State |
|-------|-------|-------|
| 0 | One month end to end | Done. 2024-06 verified on GCP: lake, Spark, BigQuery, dbt |
| 1 | Multi-year partitioned ingest | Done. 60 months, 218M rows, 58 GiB in BigQuery |
| 2 | Spark cleaning and DQ rules | 12 hard rules done; soft flags and a speed rule pending |
| 3 | dbt star schema, marts, tests, docs | Done. 5 dimensions, incremental fact, 4 marts, 56 tests |
| 4 | Airflow, incremental and idempotent | Done. 6 months run through the DAG; re-runs replace |
| 5 | CI with dbt tests; cost tuning | Done. Keyless auth, one-month build, 2.74 GiB per run |
| 6 | Dashboard and write-up | Reporting views built; Looker Studio report pending |

## Quickstart

Local, no cloud account needed:

```bash
uv sync
uv run pytest
uv run python -m lakehouse.ingest --month 2024-06
uv run python -m lakehouse.transform --month 2024-06
cat data/lake/reports/yellow/year=2024/month=06/dq_report.json
```

On GCP. Everywhere below, use the project **ID**, not its display name: the console appends
digits to the ID when a name is taken, and the wrong value fails with a confusing
`serviceusage.services.use` permission error. `gcloud projects list` shows both.

```bash
gcloud auth application-default login
```

```bash
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

One API has to be enabled by hand before the first apply. Terraform reads project metadata to
build the budget, and that read needs the very API Terraform would otherwise enable itself:

```bash
gcloud services enable cloudresourcemanager.googleapis.com --project=YOUR_PROJECT_ID
```

Then fill `infra/terraform.tfvars` from the example file and apply. It creates 14 resources:
the bucket, the dataset and its two tables, the pipeline service account and its roles, the
enabled APIs, and a budget alert.

```bash
terraform -chdir=infra apply
```

```bash
GCP_PROJECT_ID=YOUR_PROJECT_ID scripts/run_slice.sh 2024-06
```

Browse the models, tests, and lineage graph:

```bash
cd dbt && uv run dbt docs generate --profiles-dir . && uv run dbt docs serve --profiles-dir .
```

## Layout

```
src/lakehouse/    ingest, schema contract, quality rules, Spark transform, BigQuery load
schemas/bigquery/ warehouse table contracts, shared by Terraform, the loader, and tests
dbt/              seeds, staging, star schema (core), reporting marts, tests
infra/            Terraform: bucket, datasets, tables, service account, budget alert
orchestration/    Airflow image, compose file, and the monthly DAG
scripts/          source-schema survey, profiling, one-month slice runner
.github/          CI workflow: lint, tests, and a real dbt build per pull request
docs/             design decisions and data-quality rules
tests/            unit tests, including real-era Parquet fixtures
```
