output "lake_bucket" {
  value = google_storage_bucket.lake.name
}

output "curated_dataset" {
  value = google_bigquery_dataset.curated.dataset_id
}

output "pipeline_service_account" {
  value = google_service_account.pipeline.email
}

output "ci_service_account" {
  value       = google_service_account.ci.email
  description = "Pass to google-github-actions/auth as service_account."
}

output "ci_workload_identity_provider" {
  value       = google_iam_workload_identity_pool_provider.github.name
  description = "Pass to google-github-actions/auth as workload_identity_provider."
}
