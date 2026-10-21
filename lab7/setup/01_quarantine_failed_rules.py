# Databricks notebook source
# one-off DDL: run manually once before the first lab7_load run. Not part of any job.
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema_prefix", "")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix").strip()

if not CATALOG or not SCHEMA_PREFIX:
    raise ValueError("Set the catalog and schema_prefix widgets before running this notebook")

QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.dq_quarantine"

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {QUARANTINE_TABLE}
ADD COLUMN failed_rules ARRAY<STRING> COMMENT 'Names of the data-quality rules the record breaks'
""")
