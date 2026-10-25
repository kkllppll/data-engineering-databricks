# Databricks notebook source
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
sys.path.append(dbutils.widgets.get("src_path"))

from orders_pipeline.silver import add_line_number, to_quarantine, validate_order_items

BRONZE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_bronze.order_items"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.order_items"
ORDERS_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.orders"
PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.products"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.dq_quarantine"


BRONZE_COLUMNS = [
    "order_id",
    "product_id",
    "unit_price_usd",
    "quantity",
    "line_total_usd",
    "_rescued_data",
    "source_filename",
    "ingestion_timestamp",
]
SILVER_COLUMNS = [
    "order_id",
    "line_number",
    "product_id",
    "unit_price_usd",
    "quantity",
    "line_total_usd",
    "source_filename",
    "ingestion_timestamp",
]

# COMMAND ----------

checked = validate_order_items(
    spark.table(BRONZE_TABLE).select(*BRONZE_COLUMNS),
    spark.table(ORDERS_TABLE),
    spark.table(PRODUCTS_TABLE),
)

valid_items = (
    add_line_number(checked.filter("size(failed_rules) = 0"))
    .select(*SILVER_COLUMNS)
    .withColumn("processed_at", F.current_timestamp())
)
rejected_items = checked.filter("size(failed_rules) > 0")

# COMMAND ----------

(
    DeltaTable.forName(spark, SILVER_TABLE)
    .alias("t")
    .merge(
        valid_items.alias("s"),
        "t.order_id = s.order_id AND t.line_number = s.line_number",
    )
    .whenMatchedUpdateAll(condition="s.ingestion_timestamp > t.ingestion_timestamp")
    .whenNotMatchedInsertAll()
    .execute()
)

(
    to_quarantine(rejected_items, BRONZE_TABLE, BRONZE_COLUMNS)
    .write.mode("overwrite")
    .option("replaceWhere", f"source_table = '{BRONZE_TABLE}'")
    .saveAsTable(QUARANTINE_TABLE)
)
