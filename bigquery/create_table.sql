-- BigQuery table creation script for enriched engagement events

CREATE TABLE `engagement_analytics.enriched_events` (
  id INTEGER NOT NULL OPTIONS(description="Unique identifier for the engagement event"),
  content_id STRING NOT NULL OPTIONS(description="UUID of the content being engaged with"),
  user_id STRING NOT NULL OPTIONS(description="UUID of the user performing the engagement"),
  event_type STRING NOT NULL OPTIONS(description="Type of engagement event (play, pause, finish, etc.)"),
  event_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the engagement event occurred (ISO 8601 format)"),
  duration_ms INTEGER OPTIONS(description="Duration of engagement in milliseconds (null for events like 'finish')"),
  device STRING NOT NULL OPTIONS(description="Device type used for engagement (web-firefox, android, ios, etc.)"),
  raw_payload STRING NOT NULL OPTIONS(description="Raw JSON payload containing additional event metadata (session_id, user_agent, etc.)"),
  content_type STRING OPTIONS(description="Type of content (podcast, newsletter, video, etc.) - enriched from content metadata"),
  length_seconds INTEGER OPTIONS(description="Total length of content in seconds - enriched from content metadata"),
  engagement_seconds FLOAT64 OPTIONS(description="Derived field: duration_ms converted to seconds (duration_ms / 1000)"),
  engagement_pct FLOAT64 OPTIONS(description="Derived field: percentage of content consumed (engagement_seconds / length_seconds * 100)")
)
PARTITION BY DATE(event_ts)
CLUSTER BY content_type, event_type
OPTIONS(
  description="Enriched engagement events from CDC pipeline with content metadata and derived analytics fields",
  labels=[("environment", "production"), ("source", "cdc-pipeline")]
);