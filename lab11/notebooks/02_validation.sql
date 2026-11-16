-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 11: validation
-- MAGIC Checks after a producer restart: bronze keeps every delivery, silver keeps each event once and loses nothing.

-- COMMAND ----------

-- Delivery vs result: copies are expected in bronze, never in silver
SELECT 'bronze' AS layer,
       count(*) AS rows,
       count(DISTINCT event_id) AS unique_events,
       count(*) - count(DISTINCT event_id) AS extra_copies
FROM dbr_dev_ua5816bd.elina_sharabura_lab11.bronze_events
UNION ALL
SELECT 'silver',
       count(*),
       count(DISTINCT event_id),
       count(*) - count(DISTINCT event_id)
FROM dbr_dev_ua5816bd.elina_sharabura_lab11.silver_events;

-- COMMAND ----------

SELECT event_id,
       count(*) AS copies,
       collect_list(producer_id) AS producer_runs,
       min(producer_sent_at) AS first_sent,
       max(producer_sent_at) AS last_sent
FROM dbr_dev_ua5816bd.elina_sharabura_lab11.bronze_events
GROUP BY event_id
HAVING count(*) > 1
ORDER BY event_id;

-- COMMAND ----------

--each producer run numbers its records 1..n, so a gap means a record never arrived
SELECT producer_id,
       count(*) AS records,
       max(producer_seq) AS last_seq,
       max(producer_seq) - count(DISTINCT producer_seq) AS missing_seq
FROM dbr_dev_ua5816bd.elina_sharabura_lab11.bronze_events
GROUP BY producer_id
ORDER BY min(producer_sent_at);

-- COMMAND ----------

-- Reconciliation: every event from bronze is in silver, and silver has nothing bronze doesn't
WITH bronze_ids AS (
  SELECT DISTINCT event_id FROM dbr_dev_ua5816bd.elina_sharabura_lab11.bronze_events
),
silver_ids AS (
  SELECT event_id FROM dbr_dev_ua5816bd.elina_sharabura_lab11.silver_events
)
SELECT
  (SELECT count(*) FROM bronze_ids b LEFT ANTI JOIN silver_ids s USING (event_id)) AS missing_in_silver,
  (SELECT count(*) FROM silver_ids s LEFT ANTI JOIN bronze_ids b USING (event_id)) AS unknown_in_silver;

-- COMMAND ----------


SELECT round(percentile(latency_s, 0.5), 1) AS p50_seconds,
       round(percentile(latency_s, 0.95), 1) AS p95_seconds,
       round(max(latency_s), 1) AS max_seconds
FROM (
  SELECT (unix_millis(_metadata.file_modification_time) - unix_millis(producer_sent_at)) / 1000 AS latency_s
  FROM dbr_dev_ua5816bd.elina_sharabura_lab11.bronze_events
);