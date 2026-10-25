# Databricks notebook source
# Static calendar dimension, loaded for several years ahead so the daily load never needs it.
# INSERT OVERWRITE makes the load repeatable: the table always holds exactly the configured range.

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
START_DATE = dbutils.widgets.get("dim_date_start")
END_DATE = dbutils.widgets.get("dim_date_end")

DIM_DATE = f"{CATALOG}.{SCHEMA_PREFIX}_gold.dim_date"

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {DIM_DATE}
    (date_key, full_date, year, quarter, month, month_name, day_of_month, day_name, is_weekend)
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT),
    d,
    year(d),
    quarter(d),
    month(d),
    date_format(d, 'MMMM'),
    dayofmonth(d),
    date_format(d, 'EEEE'),
    dayofweek(d) IN (1, 7)
FROM (
    SELECT explode(sequence(DATE '{START_DATE}', DATE '{END_DATE}', INTERVAL 1 DAY)) AS d
)
""")
