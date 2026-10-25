# Databricks notebook source
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
sys.path.append(dbutils.widgets.get("src_path"))

from orders_pipeline.silver import CUSTOMER_RULES, add_failed_rules, keep_latest, to_quarantine

BRONZE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_bronze.customers"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.customers"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA_PREFIX}_silver.dq_quarantine"

# A change in one of these columns closes the current version and opens a new one (SCD2)
TRACKED_COLUMNS = ["name", "email", "country", "marketing_opt_in"]
BRONZE_COLUMNS = [
    "customer_id",
    "name",
    "email",
    "country",
    "age",
    "signup_date",
    "marketing_opt_in",
    "loyalty_tier",
    "_rescued_data",
    "source_filename",
    "ingestion_timestamp",
]

# COMMAND ----------

checked = add_failed_rules(spark.table(BRONZE_TABLE).select(*BRONZE_COLUMNS), CUSTOMER_RULES)

latest_customers = (
    keep_latest(
        checked.filter("size(failed_rules) = 0"),
        "customer_id",
        "ingestion_timestamp",
        "source_filename",
    )
    .select(
        "customer_id",
        "name",
        "email",
        "country",
        "age",
        "signup_date",
        "marketing_opt_in",
        "loyalty_tier",
        F.col("ingestion_timestamp").alias("effective_from"),
        "source_filename",
        "ingestion_timestamp",
    )
    .withColumn("processed_at", F.current_timestamp())
)
rejected_customers = checked.filter("size(failed_rules) > 0")

# COMMAND ----------

target = DeltaTable.forName(spark, SILVER_TABLE)
changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in TRACKED_COLUMNS)

changed_customers = (
    latest_customers.alias("s")
    .join(
        target.toDF().filter("is_current").alias("t"),
        F.col("s.customer_id") == F.col("t.customer_id"),
    )
    .filter(F.expr(changed))
    .select("s.*")
)

# Changed customers appear twice: with their key to close the current version,
# and with a NULL key so that the new version is inserted
staged = latest_customers.withColumn("merge_key", F.col("customer_id")).unionByName(
    changed_customers.withColumn("merge_key", F.lit(None).cast("int"))
)

new_version = {c: f"s.{c}" for c in latest_customers.columns}
new_version["effective_to"] = "NULL"
new_version["is_current"] = "true"

(
    target.alias("t")
    .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current")
    .whenMatchedUpdate(
        condition=changed,
        set={
            # the old version ends 1 second before the new one starts, so the intervals never overlap
            "effective_to": "s.effective_from - INTERVAL 1 SECOND",
            "is_current": "false",
            "processed_at": "s.processed_at",
        },
    )
    .whenNotMatchedInsert(values=new_version)
    .execute()
)

# COMMAND ----------

(
    to_quarantine(rejected_customers, BRONZE_TABLE, BRONZE_COLUMNS)
    .write.mode("overwrite")
    .option("replaceWhere", f"source_table = '{BRONZE_TABLE}'")
    .saveAsTable(QUARANTINE_TABLE)
)
