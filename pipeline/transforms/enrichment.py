# Stateful enrichment transform for Apache Beam pipeline
import apache_beam as beam
from apache_beam import DoFn, pvalue
from apache_beam.transforms.userstate import (
    BagStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.coders import StrUtf8Coder
import json
import logging
import datetime as dt
from typing import Tuple, Dict, Any


class EnrichWithContentStateful(DoFn):
    """Stateful DoFn for enriching engagement events with content metadata"""

    # State specifications
    content_state = ReadModifyWriteStateSpec("content_state", StrUtf8Coder())
    pending_main = BagStateSpec("pending_main", StrUtf8Coder())
    flush_timer = TimerSpec("flush", TimeDomain.REAL_TIME)

    # Side-output tag name for "first time we see this content_id"
    CACHE_NEW_TAG = "cache_new"

    def __init__(self, pending_ttl_secs: int = 10):
        self.pending_ttl_secs = pending_ttl_secs
        self.logger = logging.getLogger(self.__class__.__name__)
        self.missing_content_counter = beam.metrics.Metrics.counter(
            "enrichment", "missing_content"
        )
        self.enriched_counter = beam.metrics.Metrics.counter("enrichment", "enriched")
        self.flushed_counter = beam.metrics.Metrics.counter(
            "enrichment", "flushed_pending"
        )

    def process(
        self,
        element: Tuple[str, Tuple[str, Dict[str, Any]]],
        content_state=beam.DoFn.StateParam(content_state),
        pending_main=beam.DoFn.StateParam(pending_main),
        flush_timer=beam.DoFn.TimerParam(flush_timer),
    ):
        content_id, (tag, payload) = element
        self.logger.debug("Process key=%s tag=%s", content_id, tag)

        if tag == "content":
            # Detect first-time cache for this content_id
            prev = content_state.read()
            first_time = prev is None

            content_state.write(json.dumps(payload))
            if first_time:
                self.logger.info("New content cached: %s", content_id)

            # If this is the first time we cache this key, emit a side-output token (1)
            if first_time:
                yield pvalue.TaggedOutput(self.CACHE_NEW_TAG, 1)

            # Drain pending mains (existing behavior)
            drained_count = 0
            for raw in pending_main.read():
                drained_count += 1
                evt = json.loads(raw)
                out = self._enrich(evt, payload)
                self.enriched_counter.inc()
                yield out
            if drained_count > 0:
                pending_main.clear()
                self.flushed_counter.inc()
                self.logger.info(
                    "Enriched %d pending events for content: %s",
                    drained_count,
                    content_id,
                )

        elif tag == "main":
            raw = content_state.read()
            if raw:
                content_doc = json.loads(raw)
                out = self._enrich(payload, content_doc)
                self.enriched_counter.inc()
                self.logger.info(
                    "Enriched %s event (id=%s) for content: %s",
                    payload.get("event_type"),
                    payload.get("id"),
                    content_id,
                )
                yield out
            else:
                pending_main.add(json.dumps(payload))
                now_ms = dt.datetime.now(dt.timezone.utc).timestamp() * 1000.0
                fire_ms = int(now_ms + self.pending_ttl_secs * 1000)
                flush_timer.set(fire_ms)
                self.logger.info(
                    "Buffered %s event (id=%s) for content: %s",
                    payload.get("event_type"),
                    payload.get("id"),
                    content_id,
                )

    @on_timer(flush_timer)
    def _on_flush(
        self,
        content_state=beam.DoFn.StateParam(content_state),
        pending_main=beam.DoFn.StateParam(pending_main),
    ):
        content_raw = content_state.read()
        content_doc = json.loads(content_raw) if content_raw else None

        flush_count = 0
        missing_count = 0
        for raw_evt in pending_main.read():
            flush_count += 1
            evt = json.loads(raw_evt)
            if content_doc:
                out = self._enrich(evt, content_doc)
                self.enriched_counter.inc()
                yield out
            else:
                evt["_content_missing"] = True
                self.missing_content_counter.inc()
                missing_count += 1
                yield evt

        if flush_count > 0:
            pending_main.clear()
            if missing_count > 0:
                self.logger.warn(
                    "Flushed %d events (%d missing content)", flush_count, missing_count
                )
            else:
                self.logger.info("Flushed %d enriched events", flush_count)

    @staticmethod
    def _enrich(evt: Dict[str, Any], content: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich event with content metadata and derived fields"""
        out = dict(evt)

        # Add content metadata
        out["content_type"] = content.get("content_type")
        out["length_seconds"] = content.get("length_seconds")

        # Add derived field: engagement_seconds from duration_ms
        duration_ms = evt.get("duration_ms")
        engagement_seconds = None
        if (
            duration_ms is not None
            and isinstance(duration_ms, (int, float))
            and duration_ms >= 0
        ):
            engagement_seconds = round(
                duration_ms / 1000.0, 3
            )  # Round to 3 decimal places
            out["engagement_seconds"] = engagement_seconds
        else:
            out["engagement_seconds"] = None

        # Add derived field: engagement_pct (engagement_seconds ÷ length_seconds)
        length_seconds = content.get("length_seconds")
        if (
            engagement_seconds is not None
            and length_seconds is not None
            and isinstance(length_seconds, (int, float))
            and length_seconds > 0
        ):
            engagement_pct = (engagement_seconds / length_seconds) * 100
            out["engagement_pct"] = round(
                engagement_pct, 2
            )  # Round to 2 decimal places
        else:
            out["engagement_pct"] = None

        return out
