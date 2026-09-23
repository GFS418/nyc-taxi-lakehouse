# ---------------------------------------------------------------- streaming path
# Completed trips published to Pub/Sub are written straight into BigQuery by a BigQuery
# subscription. There is no always-on compute: nothing runs, and nothing bills, unless events flow.
# A Dataflow job would do the same with a worker running around the clock.

locals {
  pubsub_service_agent = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# The contract, enforced at publish time. A message that does not match is rejected before it
# reaches the warehouse, so schema drift fails at the edge instead of surfacing in a dashboard.
resource "google_pubsub_schema" "trip_event" {
  name       = "trip-event"
  type       = "AVRO"
  definition = file("${path.module}/../schemas/stream/trip_event.avsc")
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_topic" "trip_events" {
  name = "trip-events"

  schema_settings {
    schema   = google_pubsub_schema.trip_event.id
    encoding = "JSON"
  }

  # Keep a day of events so a rebuilt subscription can be replayed from the topic.
  message_retention_duration = "86400s"
}

# Messages BigQuery cannot write land here with the reason attached, instead of retrying forever.
resource "google_pubsub_topic" "trip_events_dead_letter" {
  name       = "trip-events-dead-letter"
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "trip_events_dead_letter_inspect" {
  name                       = "trip-events-dead-letter-inspect"
  topic                      = google_pubsub_topic.trip_events_dead_letter.id
  message_retention_duration = "604800s"
}

resource "google_bigquery_dataset" "stream" {
  dataset_id  = "stream"
  location    = var.location
  description = "Events written by the Pub/Sub BigQuery subscription. dbt deduplicates and validates them."
  depends_on  = [google_project_service.apis]
}

resource "google_bigquery_table" "trip_events" {
  dataset_id  = google_bigquery_dataset.stream.dataset_id
  table_id    = "trip_events"
  description = "Raw trip events as delivered: at least once, so duplicates are expected."
  schema      = file("${path.module}/../schemas/stream/trip_events.json")

  # Stream rows only matter until the monthly batch absorbs them. Expiring partitions caps storage.
  time_partitioning {
    type          = "DAY"
    field         = "publish_time"
    expiration_ms = 90 * 24 * 60 * 60 * 1000
  }
  clustering = ["pu_location_id"]

  deletion_protection = true
}

# Least privilege: the Pub/Sub service agent can write this one table and nothing else.
resource "google_bigquery_table_iam_member" "pubsub_writes_trip_events" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.stream.dataset_id
  table_id   = google_bigquery_table.trip_events.table_id
  role       = "roles/bigquery.dataEditor"
  member     = local.pubsub_service_agent
}

resource "google_pubsub_subscription" "trip_events_to_bigquery" {
  name  = "trip-events-to-bigquery"
  topic = google_pubsub_topic.trip_events.id

  bigquery_config {
    table            = "${var.project_id}.${google_bigquery_dataset.stream.dataset_id}.${google_bigquery_table.trip_events.table_id}"
    use_topic_schema = true
    write_metadata   = true # message_id and publish_time, which make dedup and lag measurable
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.trip_events_dead_letter.id
    max_delivery_attempts = 5
  }

  depends_on = [google_bigquery_table_iam_member.pubsub_writes_trip_events]
}

# The service agent needs these two grants to move a failing message to the dead-letter topic.
resource "google_pubsub_topic_iam_member" "dead_letter_publisher" {
  topic  = google_pubsub_topic.trip_events_dead_letter.id
  role   = "roles/pubsub.publisher"
  member = local.pubsub_service_agent
}

resource "google_pubsub_subscription_iam_member" "dead_letter_subscriber" {
  subscription = google_pubsub_subscription.trip_events_to_bigquery.id
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_service_agent
}

output "trip_events_topic" {
  value = google_pubsub_topic.trip_events.name
}
