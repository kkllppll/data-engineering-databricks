-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 10 — Setup (DDL)
-- MAGIC Connection and foreign catalog for the external Azure SQL database, and the schema for Lab 10 Delta tables.
-- MAGIC Run once manually; not part of any job.

-- COMMAND ----------

-- Credentials of the read-only user; the password comes from the Key Vault-backed secret scope.
CREATE CONNECTION IF NOT EXISTS elina_sharabura_lab10_azure_sql_conn TYPE sqlserver
OPTIONS (
  host 'sql-elinasharabura-oltp.database.windows.net',
  port '1433',
  user 'databricks_reader',
  password secret('elina_sharabura-personal-scope', 'elina-sharabura-sql-reader-password')
);

-- COMMAND ----------

CREATE FOREIGN CATALOG IF NOT EXISTS elina_sharabura_lab10_adventureworkslt
USING CONNECTION elina_sharabura_lab10_azure_sql_conn
OPTIONS (database 'sqldb-adventureworkslt');

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dbr_dev_ua5816bd.elina_sharabura_lab10
COMMENT 'Lab 10: Delta tables for the federation comparison and the CDC pipeline';