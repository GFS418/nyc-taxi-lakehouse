#!/usr/bin/env bash
# Phase 0 thin vertical slice for one month:
#   TLC CDN -> GCS raw -> Spark (canonical schema + DQ rules) -> GCS curated/quarantine
#   -> BigQuery (partition-decorator load, row counts verified) -> dbt build (models + tests)
#
# Usage: scripts/run_slice.sh 2024-06
# Needs: GCP_PROJECT_ID, `gcloud auth application-default login`, and `terraform apply` done.
# Every step is idempotent, so re-running a month replaces it and never double-counts.
set -euo pipefail

MONTH="${1:?usage: scripts/run_slice.sh YYYY-MM}"
: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
export LAKE_ROOT="${LAKE_ROOT:-gs://${GCP_PROJECT_ID}-lake}"
cd "$(dirname "$0")/.."

echo "==> ingest ${MONTH} -> ${LAKE_ROOT}"
uv run python -m lakehouse.ingest --month "${MONTH}"
echo "==> transform ${MONTH}"
uv run python -m lakehouse.transform --month "${MONTH}"
echo "==> load ${MONTH} -> ${GCP_PROJECT_ID}.curated"
uv run python -m lakehouse.load --month "${MONTH}"
echo "==> dbt build"
uv run dbt build --project-dir dbt --profiles-dir dbt
