# Databricks notebook source
# Daily sales by order country and product category, rebuilt from the fact on every run.

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")

GOLD = f"{CATALOG}.{SCHEMA_PREFIX}_gold"

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {GOLD}.agg_daily_sales
    (date_key, country_key, category, orders_count, items_count, quantity,
     gross_amount_usd, discount_amount_usd, net_amount_usd, processed_at)
SELECT
    f.date_key,
    f.country_key,
    p.category,
    COUNT(DISTINCT f.order_id),
    COUNT(*),
    SUM(f.quantity),
    CAST(SUM(f.gross_amount_usd)    AS DECIMAL(14,2)),
    CAST(SUM(f.discount_amount_usd) AS DECIMAL(14,2)),
    CAST(SUM(f.net_amount_usd)      AS DECIMAL(14,2)),
    current_timestamp()
FROM {GOLD}.fact_order_items f
JOIN {GOLD}.dim_product p ON f.product_id = p.product_id
GROUP BY f.date_key, f.country_key, p.category
""")
