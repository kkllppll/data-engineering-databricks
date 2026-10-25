# Databricks notebook source
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
sys.path.append(dbutils.widgets.get("src_path"))

from orders_pipeline.gold import (
    CHANNEL_COLUMNS,
    build_dim_customer,
    build_dim_product,
    order_channels,
    order_country_codes,
)

SILVER_CUSTOMERS = f"{CATALOG}.{SCHEMA_PREFIX}_silver.customers"
SILVER_PRODUCTS = f"{CATALOG}.{SCHEMA_PREFIX}_silver.products"
SILVER_ORDERS = f"{CATALOG}.{SCHEMA_PREFIX}_silver.orders"
DIM_CUSTOMER = f"{CATALOG}.{SCHEMA_PREFIX}_gold.dim_customer"
DIM_PRODUCT = f"{CATALOG}.{SCHEMA_PREFIX}_gold.dim_product"
DIM_COUNTRY = f"{CATALOG}.{SCHEMA_PREFIX}_gold.dim_country"
DIM_ORDER_CHANNEL = f"{CATALOG}.{SCHEMA_PREFIX}_gold.dim_order_channel"

# COMMAND ----------

build_dim_customer(spark.table(SILVER_CUSTOMERS)).writeTo(DIM_CUSTOMER).overwrite(
    F.lit(True)
)
build_dim_product(spark.table(SILVER_PRODUCTS)).writeTo(DIM_PRODUCT).overwrite(
    F.lit(True)
)

# COMMAND ----------

# Country and channel keys are identity columns referenced by the fact,
# so existing members are kept and only new ones are inserted.
orders = spark.table(SILVER_ORDERS)

(
    DeltaTable.forName(spark, DIM_COUNTRY)
    .alias("t")
    .merge(order_country_codes(orders).alias("s"), "t.country_code = s.country_code")
    .whenNotMatchedInsert(values={"country_code": "s.country_code"})
    .execute()
)

(
    DeltaTable.forName(spark, DIM_ORDER_CHANNEL)
    .alias("t")
    .merge(
        order_channels(orders).alias("s"),
        " AND ".join(f"t.{column} = s.{column}" for column in CHANNEL_COLUMNS),
    )
    .whenNotMatchedInsert(values={column: f"s.{column}" for column in CHANNEL_COLUMNS})
    .execute()
)
