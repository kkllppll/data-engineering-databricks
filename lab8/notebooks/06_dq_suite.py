# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx==0.16.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
from datetime import datetime, timezone
from string import Template

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
CHECKS_PATH = dbutils.widgets.get("checks_path")
RUN_ID = dbutils.widgets.get("run_id")
sys.path.append(dbutils.widgets.get("src_path"))

from orders_pipeline.quality import (
    blocking_failures,
    money_sum,
    reconcile,
    summarize_dqx_results,
)
from orders_pipeline.silver import keep_latest

SCHEMAS = {
    layer: f"{CATALOG}.{SCHEMA_PREFIX}_{layer}"
    for layer in ("bronze", "silver", "gold")
}
BRONZE, SILVER, GOLD = SCHEMAS["bronze"], SCHEMAS["silver"], SCHEMAS["gold"]
QUARANTINE_TABLE = f"{SILVER}.dq_quarantine"
DQ_RESULTS_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}.dq_results"

# COMMAND ----------

# Row-level and dataset-level rules from dq/checks.yml (DQX)
with open(CHECKS_PATH) as file:
    checks_by_table = yaml.safe_load(Template(file.read()).safe_substitute(SCHEMAS))

dq_engine = DQEngine(WorkspaceClient())
results = []

for table, checks in checks_by_table.items():
    status = dq_engine.validate_checks(checks)
    if status.has_errors:
        raise ValueError(f"Invalid checks for {table}: {status}")
    checked = dq_engine.apply_checks_by_metadata(spark.table(table), checks)
    results += summarize_dqx_results(checked, table, checks)

# COMMAND ----------

# reconciliation: every bronze row ends up in silver or in quarantine, and gold keeps every line and cent of silver
quarantine = spark.table(QUARANTINE_TABLE)
bronze_orders = spark.table(f"{BRONZE}.orders")
bronze_items = spark.table(f"{BRONZE}.order_items")
silver_orders = spark.table(f"{SILVER}.orders")
silver_items = spark.table(f"{SILVER}.order_items")
fact = spark.table(f"{GOLD}.fact_order_items")


def quarantined(source_table: str) -> int:
    return quarantine.filter(F.col("source_table") == source_table).count()


orders_with_items = silver_orders.join(
    silver_items.select("order_id").distinct(), "order_id", "left_semi"
)

results += [
    reconcile(
        f"{BRONZE}.orders -> {SILVER}.orders + quarantine",
        "orders_bronze_equals_silver_plus_quarantine",
        keep_latest(bronze_orders, "order_id", "ingestion_timestamp").count(),
        silver_orders.count() + quarantined(f"{BRONZE}.orders"),
    ),
    reconcile(
        f"{BRONZE}.order_items -> {SILVER}.order_items + quarantine",
        "order_items_bronze_equals_silver_plus_quarantine",
        bronze_items.count(),
        silver_items.count() + quarantined(f"{BRONZE}.order_items"),
    ),
    reconcile(
        f"{SILVER}.order_items -> {GOLD}.fact_order_items",
        "fact_keeps_every_order_line",
        silver_items.count(),
        fact.count(),
    ),
    reconcile(
        f"{SILVER}.order_items -> {GOLD}.fact_order_items",
        "fact_gross_equals_silver_line_totals",
        money_sum(silver_items, "line_total_usd"),
        money_sum(fact, "gross_amount_usd"),
    ),
    reconcile(
        f"{SILVER}.orders -> {GOLD}.fact_order_items",
        "fact_net_equals_silver_order_totals",
        money_sum(orders_with_items, "total_usd"),
        money_sum(fact, "net_amount_usd"),
    ),
]

# COMMAND ----------

# results are stored before the gate, so failed runs are kept in the history too
results_schema = spark.table(DQ_RESULTS_TABLE).schema
run_context = {"run_id": RUN_ID, "checked_at": datetime.now(timezone.utc)}

(
    spark.createDataFrame(
        [
            tuple(
                {**result, **run_context}[field.name] for field in results_schema.fields
            )
            for result in results
        ],
        results_schema,
    )
    .write.mode("append")
    .saveAsTable(DQ_RESULTS_TABLE)
)

# COMMAND ----------

failures = blocking_failures(results)
if failures:
    raise AssertionError(
        "Data-quality gate failed: "
        + "; ".join(
            f"{failure['table_name']}: {failure['check_name']}" for failure in failures
        )
    )
