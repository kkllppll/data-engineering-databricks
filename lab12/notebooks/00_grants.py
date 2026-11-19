# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Access (DCL)
# MAGIC Least privilege for the people who use the assistant: they can search the chunks and their index, nothing else
# MAGIC The raw files, the documents table, the assistant log and the evaluation tables stay with the owner:
# MAGIC the log holds the questions people ask, which may contain their data
# MAGIC Run once manually after 00_setup and 00_create_search_index, not part of any job

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")
dbutils.widgets.text("readers_group", "account users")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
readers_group = dbutils.widgets.get("readers_group")

fq_schema = f"{catalog}.{schema}"
readers = f"`{readers_group}`"

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {fq_schema} TO {readers}")

# COMMAND ----------

# the index is queried by readers, and its source table lets them see the full chunk behind a search result
spark.sql(f"GRANT SELECT ON TABLE {fq_schema}.chunks TO {readers}")
spark.sql(f"GRANT SELECT ON TABLE {fq_schema}.chunks_index TO {readers}")
