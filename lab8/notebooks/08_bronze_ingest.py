# Databricks notebook source
# Incremental ingestion of one CSV source into its bronze table with Auto Loader.
# Used by the bronze_orders, bronze_customers and bronze_products tasks with different parameters.
# Columns are inferred from the CSV header, so a file with a new column is loaded without manual
# schema changes (mergeSchema adds the column to the bronze table).
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
TABLE = dbutils.widgets.get("table")
FILE_PATTERN = dbutils.widgets.get("file_pattern")

SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA_PREFIX}/raw_data/"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA_PREFIX}/checkpoints/bronze_{TABLE}"
TARGET_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_bronze.{TABLE}"

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .option("pathGlobFilter", FILE_PATTERN)
    .load(SOURCE_PATH)
    .select(
        "*",
        F.col("_metadata.file_name").alias("source_filename"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.current_date().alias("load_date"),
    )
    .writeStream.option("checkpointLocation", f"{CHECKPOINT_PATH}/_checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
    .awaitTermination()
)
