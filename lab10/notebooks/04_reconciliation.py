# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 10, Part B: reconciliation
# MAGIC Current customers in dim_customer_scd2 have to match SalesLT.Customer in Azure SQL. Fails if they don't.

# COMMAND ----------

dbutils.widgets.text("source_catalog", "elina_sharabura_lab10_adventureworkslt")
dbutils.widgets.text(
    "target_table", "dbr_dev_ua5816bd.elina_sharabura_lab10.dim_customer_scd2"
)

# COMMAND ----------

COMPARE_SQL = """
WITH source AS (
  SELECT CustomerID AS customer_id, Title AS title, FirstName AS first_name, MiddleName AS middle_name,
         LastName AS last_name, Suffix AS suffix, CompanyName AS company_name, SalesPerson AS sales_person,
         EmailAddress AS email_address, Phone AS phone, ModifiedDate AS modified_date
  FROM IDENTIFIER(:source_catalog || '.saleslt.customer')
),
target AS (
  SELECT customer_id, title, first_name, middle_name, last_name, suffix, company_name, sales_person,
         email_address, phone, modified_date
  FROM IDENTIFIER(:target_table)
  WHERE is_current
)
SELECT 'missing in target' AS issue, * FROM (SELECT * FROM source EXCEPT SELECT * FROM target)
UNION ALL
SELECT 'extra in target' AS issue, * FROM (SELECT * FROM target EXCEPT SELECT * FROM source)
"""

target_table = dbutils.widgets.get("target_table")

differences = spark.sql(
    COMPARE_SQL,
    args={
        "source_catalog": dbutils.widgets.get("source_catalog"),
        "target_table": target_table,
    },
)

difference_count = differences.count()
if difference_count > 0:
    raise ValueError(
        f"{difference_count} rows differ between Azure SQL and {target_table}"
    )
