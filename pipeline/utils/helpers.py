# Helper functions for data parsing and formatting
import json
import datetime as dt
import logging
import os
from typing import Dict, Any

log = logging.getLogger(__name__)


def _parse_json(msg: bytes | str) -> Dict[str, Any]:
    """Parse JSON message from bytes or string"""
    if isinstance(msg, bytes):
        msg = msg.decode("utf-8")
    obj = json.loads(msg)
    return obj


def _only_after(envelope: Dict[str, Any]) -> Dict[str, Any] | None:
    """Extract 'after' payload from Debezium CDC envelope"""
    # Handle plain Debezium envelope: {"before":..., "after":...}
    if "after" in envelope:
        return envelope.get("after")

    # Handle Debezium schema/payload envelope: {"schema":..., "payload": {"before":..., "after":...}}
    payload = envelope.get("payload")
    if isinstance(payload, dict):
        return payload.get("after")

    # Unknown shape
    log.warning("Dropping envelope without 'after' in known locations: %s", envelope)
    return None


def _event_ts_iso_to_epoch_seconds(iso_ts: str) -> float:
    """Convert ISO timestamp to epoch seconds"""
    if iso_ts.endswith("Z"):
        iso_ts = iso_ts[:-1] + "+00:00"
    ts = dt.datetime.fromisoformat(iso_ts).timestamp()
    return ts


def to_redis_kv(d: Dict[str, Any]) -> tuple[str, str]:
    """Convert enriched event dict to Redis key-value pair"""
    k = f"eng:{{{d['content_id']}}}:{d['id']}"
    v = json.dumps(d, ensure_ascii=False, separators=(",", ":"))  # compact
    return (k, v)


def load_bigquery_schema() -> Dict[str, Any]:
    """Load BigQuery schema from JSON file and format for WriteToBigQuery"""
    # Get path relative to this file
    current_dir = os.path.dirname(__file__)
    schema_path = os.path.join(
        current_dir, "..", "..", "bigquery", "enriched_events_schema.json"
    )

    try:
        with open(schema_path, "r") as f:
            schema_fields = json.load(f)
        return {"fields": schema_fields}
    except FileNotFoundError:
        log.error(f"BigQuery schema file not found at: {schema_path}")
        raise
    except json.JSONDecodeError as e:
        log.error(f"Invalid JSON in BigQuery schema file: {e}")
        raise
