-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 10, Part B: initial load
-- MAGIC CDC only has changes made after it was turned on, so the current customers are loaded once from the source.
-- MAGIC Run once by hand before the first CDC merge.

-- COMMAND ----------

-- start_lsn = last LSN CDC knows about now, the merge will continue after it
-- NOT EXISTS keeps a second run from loading everything again
INSERT INTO dbr_dev_ua5816bd.elina_sharabura_lab10.dim_customer_scd2
SELECT
  c.CustomerID,
  c.Title,
  c.FirstName,
  c.MiddleName,
  c.LastName,
  c.Suffix,
  c.CompanyName,
  c.SalesPerson,
  c.EmailAddress,
  c.Phone,
  c.ModifiedDate,
  current_timestamp()      AS valid_from,
  CAST(NULL AS TIMESTAMP)  AS valid_to,
  true                     AS is_current,
  false                    AS is_deleted,
  (SELECT MAX(start_lsn) FROM elina_sharabura_lab10_adventureworkslt.cdc.lsn_time_mapping) AS start_lsn,
  CAST(NULL AS BINARY)     AS end_lsn
FROM elina_sharabura_lab10_adventureworkslt.saleslt.customer AS c
WHERE NOT EXISTS (SELECT 1 FROM dbr_dev_ua5816bd.elina_sharabura_lab10.dim_customer_scd2);