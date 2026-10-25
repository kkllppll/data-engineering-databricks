# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA_PREFIX}/raw_data/order_items/"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA_PREFIX}/checkpoints/bronze_order_items/"
TARGET_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_bronze.order_items"

SOURCE_SCHEMA = "order_id INT, product_id INT, unit_price_usd DOUBLE, quantity INT, line_total_usd DOUBLE"

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(SOURCE_SCHEMA)
    .load(SOURCE_PATH)
    .select(
        "order_id",
        "product_id",
        "unit_price_usd",
        "quantity",
        "line_total_usd",
        "_rescued_data",
        F.col("_metadata.file_name").alias("source_filename"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.current_date().alias("load_date"),
    )
    .writeStream.option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
    .awaitTermination()
)
