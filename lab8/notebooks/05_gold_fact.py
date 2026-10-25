# Databricks notebook source
import sys

from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
sys.path.append(dbutils.widgets.get("src_path"))

from orders_pipeline.gold import build_fact_order_items

SILVER_ORDER_ITEMS = f"{CATALOG}.{SCHEMA_PREFIX}_silver.order_items"
SILVER_ORDERS = f"{CATALOG}.{SCHEMA_PREFIX}_silver.orders"
DIM_CUSTOMER = f"{CATALOG}.{SCHEMA_PREFIX}_gold.dim_customer"
DIM_COUNTRY = f"{CATALOG}.{SCHEMA_PREFIX}_gold.dim_country"
DIM_ORDER_CHANNEL = f"{CATALOG}.{SCHEMA_PREFIX}_gold.dim_order_channel"
FACT_ORDER_ITEMS = f"{CATALOG}.{SCHEMA_PREFIX}_gold.fact_order_items"

# COMMAND ----------

build_fact_order_items(
    spark.table(SILVER_ORDER_ITEMS),
    spark.table(SILVER_ORDERS),
    spark.table(DIM_CUSTOMER),
    spark.table(DIM_COUNTRY),
    spark.table(DIM_ORDER_CHANNEL),
).writeTo(FACT_ORDER_ITEMS).overwrite(F.lit(True))
