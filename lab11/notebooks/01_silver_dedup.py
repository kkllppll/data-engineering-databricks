# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 11: bronze to silver without duplicates
# MAGIC Reads only new bronze rows (Structured Streaming with a checkpoint) and inserts each event into silver once.
# MAGIC Zerobus delivers at least once, so bronze can hold several copies of an event:
# MAGIC copies inside one micro-batch are removed with row_number, copies that arrive later are skipped by an insert-only MERGE on event_id.
# MAGIC The MERGE only looks at silver rows that arrived within the dedup window, so its cost doesn't grow with the whole table.

# COMMAND ----------

dbutils.widgets.text(
    "bronze_table", "dbr_dev_ua5816bd.elina_sharabura_lab11.bronze_events"
)
dbutils.widgets.text(
    "silver_table", "dbr_dev_ua5816bd.elina_sharabura_lab11.silver_events"
)
dbutils.widgets.text(
    "checkpoint_path",
    "/Volumes/dbr_dev_ua5816bd/elina_sharabura_lab11/checkpoints/silver_events",
)
dbutils.widgets.text("dedup_window_days", "7")

# COMMAND ----------

from datetime import timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
dedup_window = timedelta(days=int(dbutils.widgets.get("dedup_window_days")))

# the first copy that reached bronze is the one kept
FIRST_ARRIVAL = Window.partitionBy("event_id").orderBy(
    "producer_sent_at", "producer_id", "producer_seq"
)

# COMMAND ----------


def to_silver(batch: DataFrame) -> DataFrame:
    return (
        batch.withColumn("copy_rank", F.row_number().over(FIRST_ARRIVAL))
        .filter("copy_rank = 1")
        .select(
            "event_id",
            "session_id",
            "event_ts",
            "event_type",
            F.col("product_id").cast("bigint").alias("product_id"),
            F.col("qty").cast("int").alias("qty"),
            F.col("cart_size").cast("int").alias("cart_size"),
            "payment",
            "discount_pct",
            F.col("amount_usd").cast("decimal(12,2)").alias("amount_usd"),
            "producer_sent_at",
            F.current_timestamp().alias("processed_at"),
        )
    )


def merge_batch(batch: DataFrame, batch_id: int) -> None:
    events = to_silver(batch)
    earliest_arrival = events.agg(F.min("producer_sent_at")).first()[0]
    if earliest_arrival is None:
        return

    window_start = earliest_arrival - dedup_window
    (
        DeltaTable.forName(batch.sparkSession, silver_table)
        .alias("t")
        .merge(
            events.alias("s"),
            (F.col("t.event_id") == F.col("s.event_id"))
            & (F.col("t.producer_sent_at") >= F.lit(window_start)),
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

(
    spark.readStream.table(bronze_table)
    .writeStream.foreachBatch(merge_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
