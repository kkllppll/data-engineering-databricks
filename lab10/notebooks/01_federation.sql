-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 10 Part A: Lakehouse Federation
-- MAGIC Queries the external Azure SQL database in place through the foreign catalog
-- MAGIC `elina_sharabura_lab10_adventureworkslt`, compares federation with an ingested Delta copy
-- MAGIC and joins an external table with the Delta `dim_date`.
-- MAGIC Exploration notebook, run manually; names are fully qualified so each query shows where its data comes from.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Query external tables

-- COMMAND ----------

SHOW TABLES IN elina_sharabura_lab10_adventureworkslt.saleslt;

-- COMMAND ----------

SELECT CustomerID, FirstName, LastName, CompanyName, EmailAddress
FROM elina_sharabura_lab10_adventureworkslt.saleslt.customer
WHERE CompanyName LIKE 'Bike%'
ORDER BY CustomerID
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Query pushdown
-- MAGIC `External engine query` in the plan is the SQL actually sent to Azure SQL.

-- COMMAND ----------

-- Filter and projection: only matching rows and the two selected columns should leave Azure SQL.
EXPLAIN FORMATTED
SELECT CustomerID, CompanyName
FROM elina_sharabura_lab10_adventureworkslt.saleslt.customer
WHERE CompanyName LIKE 'Bike%';

-- COMMAND ----------

-- Aggregation: Azure SQL should return one row per ship method instead of every order.
EXPLAIN FORMATTED
SELECT ShipMethod, COUNT(*) AS orders, SUM(TotalDue) AS revenue
FROM elina_sharabura_lab10_adventureworkslt.saleslt.salesorderheader
GROUP BY ShipMethod;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Federation vs Delta copy

-- COMMAND ----------

-- one-time copy of the orders, just for the comparison
CREATE OR REPLACE TABLE dbr_dev_ua5816bd.elina_sharabura_lab10.salesorderheader_snapshot AS
SELECT *, current_timestamp() AS ingested_at
FROM elina_sharabura_lab10_adventureworkslt.saleslt.salesorderheader;

-- COMMAND ----------

-- federation
SELECT ShipMethod, COUNT(*) AS orders, ROUND(SUM(TotalDue), 2) AS revenue
FROM elina_sharabura_lab10_adventureworkslt.saleslt.salesorderheader
GROUP BY ShipMethod;

-- COMMAND ----------

-- Delta copy
SELECT ShipMethod, COUNT(*) AS orders, ROUND(SUM(TotalDue), 2) AS revenue
FROM dbr_dev_ua5816bd.elina_sharabura_lab10.salesorderheader_snapshot
GROUP BY ShipMethod;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Join with a Delta table

-- COMMAND ----------

-- orders from Azure SQL + dim_date from gold
SELECT d.year, d.quarter, d.day_name, d.is_weekend,
       COUNT(*) AS orders,
       ROUND(SUM(o.TotalDue), 2) AS revenue
FROM elina_sharabura_lab10_adventureworkslt.saleslt.salesorderheader AS o
JOIN dbr_dev_ua5816bd.elina_sharabura_gold.dim_date AS d
  ON d.full_date = CAST(o.OrderDate AS DATE)
GROUP BY d.year, d.quarter, d.day_name, d.is_weekend;