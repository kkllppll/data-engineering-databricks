# Databricks notebook source
# Business-rule CHECK constraints. Idempotent: each constraint is dropped before it is added,
# so a changed condition is applied on the next deployment.
# NOT NULL is part of the table definitions in 01_create_tables.

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

SILVER = f"{CATALOG}.{SCHEMA_PREFIX}_silver"

# fact_order_items has the Lab 6 row filter in DEV, and CHECK constraints are not allowed
# on tables with a row filter, so the fact rules are asserted by the DQ suite in both environments
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

for table, constraints in CHECK_CONSTRAINTS.items():
    for name, condition in constraints.items():
        spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({condition})")
