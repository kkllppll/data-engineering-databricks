# Databricks notebook source
# One-off DDL: run manually after a successful lab7_load run. Not part of any job.
# Re-runnable: SET NOT NULL is idempotent and every CHECK constraint is dropped before it is added.
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema_prefix", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

SILVER = f"{CATALOG}.{SCHEMA_PREFIX}_silver"
GOLD = f"{CATALOG}.{SCHEMA_PREFIX}_gold"

# CHECK passes when its condition is NULL, so every column a CHECK relies on is also NOT NULL
NOT_NULL_COLUMNS = {
    f"{SILVER}.orders": ["subtotal_usd", "discount_pct", "total_usd"],
    f"{SILVER}.order_items": ["unit_price_usd", "quantity", "line_total_usd"],
    f"{GOLD}.fact_order_items": [
        "quantity",
        "unit_price_usd",
        "discount_pct",
        "gross_amount_usd",
        "discount_amount_usd",
        "net_amount_usd",
    ],
}

# fact_order_items has a row filter (Lab 6 governance), and Databricks does not allow CHECK
# constraints on tables with row-level security; its rules are asserted in the DQ suite instead
CHECK_CONSTRAINTS = {
    f"{SILVER}.orders": {
        "chk_orders_subtotal_non_negative": "subtotal_usd >= 0",
        "chk_orders_discount_in_range": "discount_pct BETWEEN 0 AND 100",
    },
    f"{SILVER}.order_items": {
        "chk_order_items_quantity_positive": "quantity > 0",
        "chk_order_items_unit_price_positive": "unit_price_usd > 0",
    },
}

# COMMAND ----------

for table, columns in NOT_NULL_COLUMNS.items():
    for column in columns:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL")

# COMMAND ----------

for table, constraints in CHECK_CONSTRAINTS.items():
    for name, condition in constraints.items():
        spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({condition})")
