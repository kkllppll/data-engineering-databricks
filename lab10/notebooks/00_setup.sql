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


-- COMMAND ----------

-- start_lsn / end_lsn: which CDC event opened and closed the version, also used as the watermark
CREATE TABLE IF NOT EXISTS dbr_dev_ua5816bd.elina_sharabura_lab10.dim_customer_scd2 (
  customer_id     INT,
  title           STRING,
  first_name      STRING,
  middle_name     STRING,
  last_name       STRING,
  suffix          STRING,
  company_name    STRING,
  sales_person    STRING,
  email_address   STRING,
  phone           STRING,
  modified_date   TIMESTAMP,
  valid_from      TIMESTAMP,
  valid_to        TIMESTAMP,
  is_current      BOOLEAN,
  is_deleted      BOOLEAN,
  start_lsn       BINARY,
  end_lsn         BINARY
)
COMMENT 'Customers from Azure SQL with SCD2 history built from CDC';