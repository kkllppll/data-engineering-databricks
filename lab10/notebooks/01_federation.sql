-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 10 — Part A: Lakehouse Federation
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