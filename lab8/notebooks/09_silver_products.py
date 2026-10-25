# Databricks notebook source
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
sys.path.append(dbutils.widgets.get("src_path"))

from orders_pipeline.silver import PRODUCT_RULES, add_failed_rules, keep_latest, to_quarantine

BRONZE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_bronze.products"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.products"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.dq_quarantine"

TRACKED_COLUMNS = ["category", "name", "price_usd", "cost_usd", "margin_usd"]
BRONZE_COLUMNS = [
    "product_id",
    *TRACKED_COLUMNS,
    "_rescued_data",
    "source_filename",
    "ingestion_timestamp",
]

# COMMAND ----------

checked = add_failed_rules(spark.table(BRONZE_TABLE).select(*BRONZE_COLUMNS), PRODUCT_RULES)

latest_products = (
    keep_latest(
        checked.filter("size(failed_rules) = 0"),
        "product_id",
        "ingestion_timestamp",
        "source_filename",
    )
    .select("product_id", *TRACKED_COLUMNS, "source_filename", "ingestion_timestamp")
    .withColumn("processed_at", F.current_timestamp())
)
rejected_products = checked.filter("size(failed_rules) > 0")

# COMMAND ----------

# SCD1: a product row is overwritten only when one of its attributes changed
changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in TRACKED_COLUMNS)

(
    DeltaTable.forName(spark, SILVER_TABLE)
    .alias("t")
    .merge(latest_products.alias("s"), "t.product_id = s.product_id")
    .whenMatchedUpdateAll(condition=changed)
    .whenNotMatchedInsertAll()
    .execute()
)

(
    to_quarantine(rejected_products, BRONZE_TABLE, BRONZE_COLUMNS)
    .write.mode("overwrite")
    .option("replaceWhere", f"source_table = '{BRONZE_TABLE}'")
    .saveAsTable(QUARANTINE_TABLE)
)
