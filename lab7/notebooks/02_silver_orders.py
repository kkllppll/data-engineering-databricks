# Databricks notebook source
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
sys.path.append(dbutils.widgets.get("src_path"))

from orders_pipeline.silver import to_quarantine, validate_orders

BRONZE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_bronze.orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.orders"
CUSTOMERS_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.customers"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.dq_quarantine"


SILVER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_time",
    "payment_method",
    "discount_pct",
    "subtotal_usd",
    "total_usd",
    "country",
    "device",
    "source",
    "source_filename",
    "ingestion_timestamp",
]

# COMMAND ----------

checked = validate_orders(spark.table(BRONZE_TABLE), spark.table(CUSTOMERS_TABLE))

valid_orders = (
    checked.filter("size(failed_rules) = 0")
    .select(*SILVER_COLUMNS)
    .withColumn("processed_at", F.current_timestamp())
)
rejected_orders = checked.filter("size(failed_rules) > 0")

# COMMAND ----------

(
    DeltaTable.forName(spark, SILVER_TABLE)
    .alias("t")
    .merge(valid_orders.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll(condition="s.ingestion_timestamp > t.ingestion_timestamp")
    .whenNotMatchedInsertAll()
    .execute()
)

(
    to_quarantine(rejected_orders, BRONZE_TABLE, SILVER_COLUMNS)
    .write.mode("overwrite")
    .option("replaceWhere", f"source_table = '{BRONZE_TABLE}'")
    .saveAsTable(QUARANTINE_TABLE)
)
