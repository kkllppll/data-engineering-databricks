# Databricks notebook source
# One-off DDL: run manually once before the first lab7_dq_suite run. Not part of any job
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema_prefix", "")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix").strip()

if not CATALOG or not SCHEMA_PREFIX:
    raise ValueError(
        "Set the catalog and schema_prefix widgets before running this notebook"
    )

DQ_RESULTS_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}.dq_results"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_RESULTS_TABLE} (
    run_id         STRING        NOT NULL COMMENT 'Databricks job run id; groups the checks of one suite run',
    checked_at     TIMESTAMP     NOT NULL,
    check_type     STRING        NOT NULL COMMENT 'dqx or reconciliation',
    table_name     STRING        NOT NULL COMMENT 'Checked table, or source -> target for reconciliation',
    check_name     STRING        NOT NULL,
    dimension      STRING        NOT NULL COMMENT 'completeness, uniqueness, validity, consistency, timeliness or reconciliation',
    criticality    STRING        NOT NULL COMMENT 'error fails the suite, warn is only recorded',
    failed_rows    BIGINT        COMMENT 'Rows that break a dqx check',
    total_rows     BIGINT,
    expected_value DECIMAL(22,2) COMMENT 'Reconciliation: value on the source side',
    actual_value   DECIMAL(22,2) COMMENT 'Reconciliation: value on the target side',
    passed         BOOLEAN       NOT NULL
)
COMMENT 'History of data-quality results: one row per check per lab7_dq_suite run'
""")
