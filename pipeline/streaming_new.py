# Main streaming enrichment pipeline
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.transforms import window, trigger
import argparse
import logging

from .config import (
    REDIS_CONFIG,
    BIGQUERY_CONFIG,
    THIRD_PARTY_API_CONFIG,
    DEFAULT_EMIT_INTERVAL_SECS,
    PIPELINE_CONFIG,
)
from .transforms import EnrichWithContentStateful, CacheSizeLogger, WriteToRedisDoFn
from .transforms.http_sender import HTTPSenderDoFn

from .utils import (
    _parse_json,
    _only_after,
    _event_ts_iso_to_epoch_seconds,
    to_redis_kv,
    load_bigquery_schema,
)

# Logging Confgis
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s",
)
log = logging.getLogger("enrich_pipeline")


def build_pipeline(
    p: beam.Pipeline,
    main_subscription: str,
    content_subscription: str,
    emit_every_secs: int = DEFAULT_EMIT_INTERVAL_SECS,
) -> beam.PCollection:
    """Build the streaming enrichment pipeline"""

    # 1. Read and process main engagement events
    main_after = (
        p
        | "ReadMain" >> beam.io.ReadFromPubSub(subscription=main_subscription)
        | "ParseMain" >> beam.Map(_parse_json)
        | "AfterMain" >> beam.Map(_only_after)
        | "DropNullMain" >> beam.Filter(lambda x: x is not None)
        | "StampMainEventTime"
        >> beam.Map(
            lambda rec: beam.window.TimestampedValue(
                rec, _event_ts_iso_to_epoch_seconds(rec["event_ts"])
            )
        )
        | "KeyByContent_Main"
        >> beam.Map(lambda rec: (rec["content_id"], ("main", rec)))
    )

    # 2. Read and process content events
    content_after = (
        p
        | "ReadContent" >> beam.io.ReadFromPubSub(subscription=content_subscription)
        | "ParseContent" >> beam.Map(_parse_json)
        | "AfterContent" >> beam.Map(_only_after)
        | "DropNullContent" >> beam.Filter(lambda x: x is not None)
        | "StampContentEventTime"
        >> beam.Map(
            lambda rec: beam.window.TimestampedValue(
                rec,
                _event_ts_iso_to_epoch_seconds(
                    rec.get("publish_ts", "2024-01-01T00:00:00Z")
                ),
            )
        )
        | "KeyByContent_Content" >> beam.Map(lambda rec: (rec["id"], ("content", rec)))
    )

    # 3. Merge streams and apply windowing for stateful processing
    merged = (main_after, content_after) | "MergeStreams" >> beam.Flatten()

    merged = merged | "PreState_NonDefaultTrigger" >> beam.WindowInto(
        window.GlobalWindows(),
        trigger=trigger.Repeatedly(trigger.AfterProcessingTime(emit_every_secs)),
        accumulation_mode=trigger.AccumulationMode.DISCARDING,
        allowed_lateness=0,
    )

    # 4. Stateful enrichment with side outputs
    enrich_outputs = merged | "StatefulEnrich" >> beam.ParDo(
        EnrichWithContentStateful(pending_ttl_secs=10)
    ).with_outputs(EnrichWithContentStateful.CACHE_NEW_TAG, main="enriched")
    enriched = enrich_outputs.enriched
    cache_new = enrich_outputs[EnrichWithContentStateful.CACHE_NEW_TAG]

    # 5. Contnet Cache size logging
    _ = (
        cache_new
        | "CacheLog_WindowFix"
        >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING,
        )
        | "ToSingletonKey" >> beam.Map(lambda _: ("__cache__", 1))
        | "CacheSizeLogger" >> beam.ParDo(CacheSizeLogger())
    )

    # 6. Emit enriched events periodically
    emitted = enriched | "EmitEveryN_ProcessingTime" >> beam.WindowInto(
        window.GlobalWindows(),
        trigger=trigger.Repeatedly(trigger.AfterProcessingTime(emit_every_secs)),
        accumulation_mode=trigger.AccumulationMode.DISCARDING,
    )

    # 7. Write to Redis
    _ = (
        emitted
        | "ToRedisKV" >> beam.Map(to_redis_kv)
        | "WriteToRedis"
        >> beam.ParDo(
            WriteToRedisDoFn(
                host=REDIS_CONFIG["host"],
                port=REDIS_CONFIG["port"],
                ttl=REDIS_CONFIG["ttl"],
            )
        )
    )

    # 8. Write to BigQuery
    # Load schema from JSON file
    table_schema = load_bigquery_schema()

    table_spec = f"{BIGQUERY_CONFIG['project']}:{BIGQUERY_CONFIG['dataset']}.{BIGQUERY_CONFIG['table']}"
    _ = emitted | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
        table=table_spec,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
    )

    # 9. Send to third-party API
    _ = emitted | "SendToThirdPartyAPI" >> beam.ParDo(
        HTTPSenderDoFn(
            api_url=THIRD_PARTY_API_CONFIG["url"],
            timeout=THIRD_PARTY_API_CONFIG["timeout"],
        )
    )

    # 10. Write to console JSON for debugging
    # _ = emitted | "LogJSON" >> beam.Map(lambda d: print(json.dumps(d), flush=True))

    return emitted


def main():
    """Main pipeline entry point"""
    # Parse only pipeline-specific options, not our subscription args
    parser = argparse.ArgumentParser()
    args, pipeline_argv = parser.parse_known_args()

    # Logging at INFO
    logging.getLogger().setLevel(logging.INFO)
    log.info("Starting pipeline with configuration:")
    log.info("  Main subscription: %s", PIPELINE_CONFIG["main_subscription"])
    log.info("  Content subscription: %s", PIPELINE_CONFIG["content_subscription"])
    log.info("  Emit interval: %s seconds", PIPELINE_CONFIG["emit_interval"])

    options = PipelineOptions(pipeline_argv)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        build_pipeline(
            p,
            main_subscription=PIPELINE_CONFIG["main_subscription"],
            content_subscription=PIPELINE_CONFIG["content_subscription"],
            emit_every_secs=PIPELINE_CONFIG["emit_interval"],
        )


if __name__ == "__main__":
    main()
