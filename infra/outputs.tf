output "lake_bucket" {
  value = google_storage_bucket.lake.name
}

output "curated_dataset" {
  value = google_bigquery_dataset.curated.dataset_id
}

output "pipeline_service_account" {
  value = google_service_account.pipeline.email
}
