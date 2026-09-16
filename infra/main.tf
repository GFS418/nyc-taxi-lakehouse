provider "google" {
  project = var.project_id
  region  = var.region

  # Billing Budgets API calls with user ADC need an explicit quota project.
  billing_project       = var.project_id
  user_project_override = true
}

data "google_project" "this" {}

locals {
  apis = [
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "billingbudgets.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

# ---------------------------------------------------------------- data lake
resource "google_storage_bucket" "lake" {
  name                        = "${var.project_id}-lake"
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  # Pipeline writes are idempotent overwrites, and every raw file can be re-downloaded
  # from TLC. Soft-delete would bill each overwritten month for 7 extra days.
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  depends_on = [google_project_service.apis]
}

# ---------------------------------------------------------------- warehouse
resource "google_bigquery_dataset" "curated" {
  dataset_id  = "curated"
  location    = var.location
  description = "Spark-curated trip data loaded from the lake. dbt sources read from here; dbt owns its own datasets."

  # Bill compressed (physical) bytes, not logical bytes. Logical size is fixed by column types
  # (NUMERIC is 16 bytes per value): about 58 GiB for 2019-2023. Columnar compression should
  # keep physical bytes near the 10 GiB free tier. Verify with INFORMATION_SCHEMA.TABLE_STORAGE
  # after the backfill; the model can only be changed once every 14 days.
  storage_billing_model = "PHYSICAL"

  # Physical billing charges for time-travel bytes. Two days is the minimum and is enough to
  # recover from a bad load; every partition can also be rebuilt from the lake.
  max_time_travel_hours = "48"

  depends_on = [google_project_service.apis]
}

# One partition per TLC file month. Partition decorators (yellow_trips$YYYYMM) with
# WRITE_TRUNCATE make every monthly load an idempotent replace of exactly that month.
resource "google_bigquery_table" "yellow_trips" {
  dataset_id  = google_bigquery_dataset.curated.dataset_id
  table_id    = "yellow_trips"
  description = "One row per trip that passed every hard data-quality rule."
  schema      = file("${path.module}/../schemas/bigquery/yellow_trips.json")

  time_partitioning {
    type  = "MONTH"
    field = "pickup_datetime"
  }
  clustering = ["pu_location_id", "do_location_id"]

  deletion_protection = true
}

# Partitioned by source_month, not pickup time: out-of-period rows are exactly the
# ones whose pickup month disagrees with their file, and they must still land in
# the partition of the month that produced them.
resource "google_bigquery_table" "yellow_trips_quarantine" {
  dataset_id  = google_bigquery_dataset.curated.dataset_id
  table_id    = "yellow_trips_quarantine"
  description = "Rows rejected by hard data-quality rules, with every failed rule recorded."
  schema      = file("${path.module}/../schemas/bigquery/yellow_trips_quarantine.json")

  time_partitioning {
    type  = "MONTH"
    field = "source_month"
  }
  clustering = ["reject_reason"]

  deletion_protection = true
}

# ---------------------------------------------------------------- identity
# Used later by Airflow and CI. No key is created here: keys in Terraform end up
# in plaintext state.
resource "google_service_account" "pipeline" {
  account_id   = "lakehouse-pipeline"
  display_name = "NYC taxi lakehouse pipeline (Airflow + CI)"
  depends_on   = [google_project_service.apis]
}

resource "google_storage_bucket_iam_member" "pipeline_lake" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bq" {
  for_each = toset([
    "roles/bigquery.jobUser",    # run load and query jobs
    "roles/bigquery.dataEditor", # write curated tables; lets dbt create its own datasets
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# ---------------------------------------------------------------- cost guardrail
resource "google_billing_budget" "monthly" {
  billing_account = var.billing_account_id
  display_name    = "nyc-taxi-lakehouse monthly"

  budget_filter {
    projects = ["projects/${data.google_project.this.number}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.monthly_budget_usd)
    }
  }

  threshold_rules { threshold_percent = 0.5 }
  threshold_rules { threshold_percent = 0.9 }
  threshold_rules { threshold_percent = 1.0 }

  depends_on = [google_project_service.apis]
}
