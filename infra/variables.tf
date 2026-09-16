variable "project_id" {
  description = "GCP project that holds the lake and warehouse."
  type        = string
}

variable "billing_account_id" {
  description = "Billing account ID (XXXXXX-XXXXXX-XXXXXX) the budget alert attaches to."
  type        = string
}

variable "location" {
  description = <<-EOT
    One region for BOTH the bucket and the datasets: BigQuery load jobs require them to be
    colocated. us-central1 is in the Cloud Storage free tier (multi-region US is not) and has
    the same BigQuery prices as the US multi-region.
  EOT
  type        = string
  default     = "us-central1"
}

variable "region" {
  description = "Default region for regional resources."
  type        = string
  default     = "us-central1"
}

variable "monthly_budget_usd" {
  description = "Budget alert threshold in USD. Alerts only: a GCP budget never caps spend."
  type        = number
  default     = 10
}
