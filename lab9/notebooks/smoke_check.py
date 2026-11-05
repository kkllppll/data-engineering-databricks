# Databricks notebook source
# Smoke check after the daily run: every gold table of the orders star schema must contain data
import logging

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger("smoke_check")
logger.setLevel(logging.INFO)

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

GOLD = f"{CATALOG}.{SCHEMA_PREFIX}_gold"
GOLD_TABLES = [
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_country",
    "dim_order_channel",
    "fact_order_items",
    "agg_daily_sales",
]

# COMMAND ----------

empty_tables = [
    table for table in GOLD_TABLES if spark.table(f"{GOLD}.{table}").isEmpty()
]

if empty_tables:
    raise RuntimeError(f"Smoke check failed, empty gold tables: {empty_tables}")

logger.info(
    "Smoke check passed: all %d gold tables in %s contain data", len(GOLD_TABLES), GOLD
)
