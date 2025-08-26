# Configuration settings for the streaming pipeline
import os

# Set up Pub/Sub emulator environment
pubsub_host = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
os.environ["PUBSUB_EMULATOR_HOST"] = pubsub_host

# Test Project
PROJECT_ID = os.environ.get("PUBSUB_PROJECT_ID", "streaming-project")

# Database connection config
DB_CONFIG = {
    'host': os.environ.get("DB_HOST", "localhost"),
    'port': int(os.environ.get("DB_PORT", "5432")),
    'database': os.environ.get("DB_NAME", "users_data"),
    'user': os.environ.get("DB_USER", "debezium"),
    'password': os.environ.get("DB_PASSWORD", "dbz")
}

# Redis configuration
REDIS_CONFIG = {
    'host': os.environ.get("REDIS_HOST", "localhost"),
    'port': int(os.environ.get("REDIS_PORT", "6379")),
    'ttl': 3600  # 1 hour TTL
}

# BigQuery configuration
BIGQUERY_CONFIG = {
    'project': os.environ.get("BIGQUERY_PROJECT", "dev-data-infra"),
    'dataset': os.environ.get("BIGQUERY_DATASET", "engagement_analytics"),
    'table': os.environ.get("BIGQUERY_TABLE", "enriched_events"),
    'write_disposition': 'WRITE_APPEND',
    'create_disposition': 'CREATE_IF_NEEDED'
}

# Third-party API configuration
THIRD_PARTY_API_CONFIG = {
    'url': os.environ.get("THIRD_PARTY_API_URL", "http://localhost:8080"),
    'timeout': int(os.environ.get("THIRD_PARTY_API_TIMEOUT", "30"))
}

# Default pipeline Window size
DEFAULT_EMIT_INTERVAL_SECS = 3

# Pipeline subscription configuration
PIPELINE_CONFIG = {
    'main_subscription': f"projects/{PROJECT_ID}/subscriptions/cdc-engagement_events-sub",
    'content_subscription': f"projects/{PROJECT_ID}/subscriptions/cdc-content-sub",
    'emit_interval': int(os.environ.get("DEFAULT_EMIT_INTERVAL_SECS", "3"))
}
