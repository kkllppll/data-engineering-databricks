from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SCHEMA = spark.conf.get("silver.schema")

WIKIPEDIA_EVENT_SCHEMA = (
    "id BIGINT, type STRING, namespace INT, title STRING, `user` STRING, bot BOOLEAN, "
    "minor BOOLEAN, wiki STRING, server_name STRING, `timestamp` BIGINT, "
    "length STRUCT<old: INT, new: INT>"
)
# Event Hub delivers at-least-once, retried sends arrive within seconds, so one hour of state is ample
DEDUPLICATION_WINDOW = "1 hour"

WIKIPEDIA_DROP_RULES = {
    "parsable_payload": "event_id IS NOT NULL",
    "has_title": "title IS NOT NULL",
    "has_wiki": "wiki IS NOT NULL",
    "valid_event_time": "event_time IS NOT NULL",
}


@dp.table(
    name=f"{SILVER_SCHEMA}.wikipedia_recentchange",
    comment="Parsed and validated Wikipedia recent-change events, deduplicated by event id",
)
@dp.expect_all_or_drop(WIKIPEDIA_DROP_RULES)
@dp.expect(
    "known_event_type", "event_type IN ('edit', 'new', 'log', 'categorize', 'external')"
)
def silver_wikipedia_recentchange():
    return (
        spark.readStream.table("wikipedia_recentchange")
        .withColumn("event", F.from_json("raw_payload", WIKIPEDIA_EVENT_SCHEMA))
        .select(
            F.col("event.id").alias("event_id"),
            F.col("event.type").alias("event_type"),
            F.col("event.namespace").alias("namespace"),
            F.col("event.title").alias("title"),
            F.col("event.user").alias("user_name"),
            F.col("event.bot").alias("is_bot"),
            F.col("event.minor").alias("is_minor_edit"),
            F.col("event.wiki").alias("wiki"),
            F.col("event.server_name").alias("server_name"),
            F.timestamp_seconds("event.timestamp").alias("event_time"),
            (F.col("event.length.new") - F.col("event.length.old")).alias(
                "length_delta"
            ),
            "eventhub_enqueued_at",
            "ingestion_timestamp",
        )
        .withWatermark("eventhub_enqueued_at", DEDUPLICATION_WINDOW)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
