# enrich_stream.py
# Python 3.11 / Beam 2.66.0
import json
import argparse
import datetime as dt
import logging
from typing import Tuple, Dict, Any
import redis

import apache_beam as beam
from apache_beam import DoFn, PCollection
from apache_beam import pvalue  # [ADDED] side outputs
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import window, trigger

# ---- user state + timers (Python SDK) ----
from apache_beam.transforms.userstate import (
    BagStateSpec,
    ReadModifyWriteStateSpec,  # single-value state in Python SDK
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain  # Beam 2.66
from apache_beam.coders import StrUtf8Coder
from apache_beam.coders import VarIntCoder  # [ADDED] for cache-size state

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s",
)
log = logging.getLogger("enrich_pipeline")


# ---------- Helpers ----------
def _parse_json(msg: bytes | str) -> Dict[str, Any]:
    if isinstance(msg, bytes):
        msg = msg.decode("utf-8")
    obj = json.loads(msg)
    return obj

def _only_after(envelope: Dict[str, Any]) -> Dict[str, Any] | None:
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
    if iso_ts.endswith("Z"):
        iso_ts = iso_ts[:-1] + "+00:00"
    ts = dt.datetime.fromisoformat(iso_ts).timestamp()
    return ts


# ---------- Stateful enrichment ----------
class EnrichWithContentStateful(DoFn):
    # latest content JSON (UTF-8 string)
    content_state = ReadModifyWriteStateSpec('content_state', StrUtf8Coder())
    # buffered main events awaiting content
    pending_main = BagStateSpec('pending_main', StrUtf8Coder())
    # processing-time flush timer
    flush_timer  = TimerSpec('flush', TimeDomain.REAL_TIME)

    # [ADDED] side-output tag name for "first time we see this content_id"
    CACHE_NEW_TAG = 'cache_new'

    def __init__(self, pending_ttl_secs: int = 10):
        self.pending_ttl_secs = pending_ttl_secs
        self.logger = logging.getLogger(self.__class__.__name__)
        self.missing_content_counter = beam.metrics.Metrics.counter("enrichment", "missing_content")
        self.enriched_counter = beam.metrics.Metrics.counter("enrichment", "enriched")
        self.flushed_counter = beam.metrics.Metrics.counter("enrichment", "flushed_pending")

    def process(
        self,
        element: Tuple[str, Tuple[str, Dict[str, Any]]],
        content_state=beam.DoFn.StateParam(content_state),
        pending_main=beam.DoFn.StateParam(pending_main),
        flush_timer=beam.DoFn.TimerParam(flush_timer)
    ):
        content_id, (tag, payload) = element
        self.logger.debug("Process key=%s tag=%s", content_id, tag)

        if tag == "content":
            # [ADDED] detect first-time cache for this content_id
            prev = content_state.read()
            first_time = (prev is None)

            content_state.write(json.dumps(payload))
            if first_time:
                self.logger.info("New content cached: %s", content_id)

            # [ADDED] if this is the first time we cache this key, emit a side-output token (1)
            if first_time:
                yield pvalue.TaggedOutput(self.CACHE_NEW_TAG, 1)

            # drain pending mains (existing behavior)
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
                self.logger.info("Enriched %d pending events for content: %s", drained_count, content_id)

        elif tag == "main":
            raw = content_state.read()
            if raw:
                content_doc = json.loads(raw)
                out = self._enrich(payload, content_doc)
                self.enriched_counter.inc()
                yield out
            else:
                pending_main.add(json.dumps(payload))
                now_ms = dt.datetime.now(dt.timezone.utc).timestamp() * 1000.0
                fire_ms = int(now_ms + self.pending_ttl_secs * 1000)
                flush_timer.set(fire_ms)
                self.logger.debug("Buffered event for content: %s", content_id)

    @on_timer(flush_timer)  # Beam 2.66: pass TimerSpec
    def _on_flush(self, content_state=beam.DoFn.StateParam(content_state),
                  pending_main=beam.DoFn.StateParam(pending_main)):
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
                self.logger.warn("Flushed %d events (%d missing content)", flush_count, missing_count)
            else:
                self.logger.info("Flushed %d enriched events", flush_count)

    @staticmethod
    def _enrich(evt: Dict[str, Any], content: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(evt)
        out["content_type"] = content.get("content_type")
        out["length_seconds"] = content.get("length_seconds")
        return out


# ---------- Cache size logger (stateful, immediate) ----------
class CacheSizeLogger(DoFn):  # [ADDED]
    """
    Keeps a single global counter in per-key state and logs immediately.
    Input elements should all share the same key, e.g. ('__cache__', 1)
    """
    count_state = ReadModifyWriteStateSpec('cache_count', VarIntCoder())

    def process(self, kv, count_state=beam.DoFn.StateParam(count_state)):
        key, inc = kv  # key is constant '__cache__', inc is 1
        current = count_state.read() or 0
        current += int(inc)
        count_state.write(current)
        log.info("Content cache size: %d unique items", current)
        # No output needed - just return to satisfy Beam's iterator requirement
        return

#-------------------------- Redis
class WriteToRedisDoFn(beam.DoFn):
    def __init__(self, host, port, password=None, ttl=None):
        self._host = host
        self._port = port
        self._password = password
        self._ttl = ttl
        self._redis_client = None
        
        # Initialize metrics and logger
        self._ok = beam.metrics.Metrics.counter("redis", "writes_ok")
        self._err = beam.metrics.Metrics.counter("redis", "writes_error")
        self._log = logging.getLogger(self.__class__.__name__)

    def setup(self):
        """Initializes the Redis client once per worker/process."""
        import redis
        self._redis_client = redis.Redis(
            host=self._host, port=self._port, password=self._password, decode_responses=True
        )

    def process(self, element):
        k, v = element
        try:
            if self._ttl:
                self._redis_client.set(k, v, ex=self._ttl)
            else:
                self._redis_client.set(k, v)
            self._ok.inc()
        except Exception as e:
            self._err.inc()
            self._log.error("Redis write failed for key=%s: %s", k, e)
        
        # Yield nothing to satisfy Beam's iterator requirement
        return

    def teardown(self):
        """Closes the Redis connection (optional, as redis-py handles connection pooling)."""
        if self._redis_client:
            self._redis_client.connection_pool.disconnect()

def to_redis_kv(d):
    k = f"eng:{{{d['content_id']}}}:{d['id']}"
    v = json.dumps(d, ensure_ascii=False, separators=(",", ":"))  # compact
    return (k, v)

# ---------- Pipeline ----------
def build_pipeline(
    p: beam.Pipeline,
    main_subscription: str,
    content_subscription: str,
    emit_every_secs: int
) -> PCollection:

    main_after = (
        p
        | "ReadMain" >> beam.io.ReadFromPubSub(subscription=main_subscription)
        | "ParseMain" >> beam.Map(_parse_json)
        | "AfterMain" >> beam.Map(_only_after)
        | "DropNullMain" >> beam.Filter(lambda x: x is not None)
        | "StampMainEventTime" >> beam.Map(
            lambda rec: beam.window.TimestampedValue(rec, _event_ts_iso_to_epoch_seconds(rec["event_ts"]))
        )
        | "KeyByContent_Main" >> beam.Map(lambda rec: (rec["content_id"], ("main", rec)))
    )

    content_after = (
        p
        | "ReadContent" >> beam.io.ReadFromPubSub(subscription=content_subscription)
        | "ParseContent" >> beam.Map(_parse_json)
        | "AfterContent" >> beam.Map(_only_after)
        | "DropNullContent" >> beam.Filter(lambda x: x is not None)
        | "StampContentEventTime" >> beam.Map(
            lambda rec: beam.window.TimestampedValue(
                rec, _event_ts_iso_to_epoch_seconds(rec.get("publish_ts", dt.datetime.now(dt.timezone.utc).isoformat()))
            )
        )
        | "KeyByContent_Content" >> beam.Map(lambda rec: (rec["id"], ("content", rec)))
    )

    merged = (main_after, content_after) | "MergeStreams" >> beam.Flatten()

    # Minimal fix: GlobalWindow + non-default trigger before stateful ParDo
    merged = (
        merged
        | "PreState_NonDefaultTrigger" >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING,
            allowed_lateness=0
        )
    )

    # Capture enriched output + side-output for "new cache keys"
    enrich_outputs = merged | "StatefulEnrich" >> beam.ParDo(
        EnrichWithContentStateful(pending_ttl_secs=10)
    ).with_outputs(
        EnrichWithContentStateful.CACHE_NEW_TAG,  # side output tag
        main="enriched"
    )
    enriched = enrich_outputs.enriched
    cache_new = enrich_outputs[EnrichWithContentStateful.CACHE_NEW_TAG]  # stream: one element per first-time key

    # Immediate cache-size logging using a stateful singleton counter
    _ = (
        cache_new
        | "CacheLog_WindowFix" >> beam.WindowInto(  # ensure non-default trigger for legality + immediacy
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
        | "ToSingletonKey" >> beam.Map(lambda _: ("__cache__", 1))
        | "CacheSizeLogger" >> beam.ParDo(CacheSizeLogger())
    )

    # Emit every N seconds using PROCESSING TIME ONLY (includes everything that arrives)
    emitted = (
        enriched
        | "EmitEveryN_ProcessingTime" >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterProcessingTime(emit_every_secs)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
    )

    # log enriched records at info
    # _ = emitted | "LogJSON" >> beam.Map(lambda d: print(json.dumps(d)))

    # write to redis
    Redis = (
        emitted 
        | "ToRedisKV" >> beam.Map(to_redis_kv)
        | 'Write to Redis' >> beam.ParDo(WriteToRedisDoFn(host='localhost', port=6379, ttl=3600))  # 1 hour TTL
    )
    _ = Redis

    return emitted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--main_subscription", required=True)
    parser.add_argument("--content_subscription", required=True)
    parser.add_argument("--emit_every_secs", type=int, default=5) # 5 seconds
    args, pipeline_argv = parser.parse_known_args()

    # Beam / Python logging at INFO
    logging.getLogger().setLevel(logging.INFO)
    log.info("Starting pipeline with args=%s", args)

    options = PipelineOptions(pipeline_argv)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        build_pipeline(
            p,
            main_subscription=args.main_subscription,
            content_subscription=args.content_subscription,
            emit_every_secs=args.emit_every_secs,
        )

    log.info("Pipeline finished (this line may not appear for long-running streaming jobs).")


if __name__ == "__main__":
    main()

