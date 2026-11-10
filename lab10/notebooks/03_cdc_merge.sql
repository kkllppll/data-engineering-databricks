-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 10, Part B: CDC merge
-- MAGIC Takes new CDC events for SalesLT.Customer and applies them to dim_customer_scd2 in one MERGE.
-- MAGIC Insert and update make a new version, delete closes the version and marks it as deleted.

-- COMMAND ----------

CREATE WIDGET TEXT source_catalog DEFAULT 'elina_sharabura_lab10_adventureworkslt';
CREATE WIDGET TEXT target_table DEFAULT 'dbr_dev_ua5816bd.elina_sharabura_lab10.dim_customer_scd2';

-- COMMAND ----------

MERGE INTO IDENTIFIER(:target_table) AS t
USING (
  WITH new_events AS (
    -- only events after the last LSN that is already in the target
    SELECT
      ct.`__$start_lsn`  AS lsn,
      ct.`__$seqval`     AS seqval,
      ct.`__$operation`  AS op,
      m.tran_end_time    AS change_time,
      ct.CustomerID      AS customer_id,
      ct.Title           AS title,
      ct.FirstName       AS first_name,
      ct.MiddleName      AS middle_name,
      ct.LastName        AS last_name,
      ct.Suffix          AS suffix,
      ct.CompanyName     AS company_name,
      ct.SalesPerson     AS sales_person,
      ct.EmailAddress    AS email_address,
      ct.Phone           AS phone,
      ct.ModifiedDate    AS modified_date
    FROM IDENTIFIER(:source_catalog || '.cdc.saleslt_customer_ct') AS ct
    JOIN IDENTIFIER(:source_catalog || '.cdc.lsn_time_mapping') AS m
      ON m.start_lsn = ct.`__$start_lsn`
    WHERE ct.`__$operation` IN (1, 2, 4)  -- 3 is the old value of an update, not needed
      AND ct.`__$start_lsn` > (
        SELECT coalesce(greatest(max(start_lsn), max(end_lsn)), X'00')
        FROM IDENTIFIER(:target_table)
      )
  ),
  ordered AS (
    SELECT *,
           ROW_NUMBER()      OVER w AS event_no,
           LEAD(op)          OVER w AS next_op,
           LEAD(change_time) OVER w AS next_change_time,
           LEAD(lsn)         OVER w AS next_lsn
    FROM new_events
    WINDOW w AS (PARTITION BY customer_id ORDER BY lsn, seqval)
  )
  -- first event of a customer closes the version that is open in the target
  SELECT customer_id AS merge_key, op, change_time, lsn,
         customer_id, title, first_name, middle_name, last_name, suffix,
         company_name, sales_person, email_address, phone, modified_date,
         CAST(NULL AS TIMESTAMP) AS valid_from,
         CAST(NULL AS TIMESTAMP) AS valid_to,
         CAST(NULL AS BOOLEAN)   AS is_current,
         CAST(NULL AS BOOLEAN)   AS is_deleted,
         CAST(NULL AS BINARY)    AS end_lsn
  FROM ordered
  WHERE event_no = 1

  UNION ALL

  -- every insert and update becomes a version, closed by the next event if there is one
  SELECT CAST(NULL AS INT) AS merge_key, op, change_time, lsn,
         customer_id, title, first_name, middle_name, last_name, suffix,
         company_name, sales_person, email_address, phone, modified_date,
         change_time                            AS valid_from,
         next_change_time - INTERVAL 1 SECOND   AS valid_to,
         next_change_time IS NULL               AS is_current,
         coalesce(next_op = 1, false)           AS is_deleted,
         next_lsn                               AS end_lsn
  FROM ordered
  WHERE op IN (2, 4)
) AS s
ON t.customer_id = s.merge_key AND t.is_current
WHEN MATCHED THEN UPDATE SET
  t.valid_to   = s.change_time - INTERVAL 1 SECOND,
  t.is_current = false,
  t.is_deleted = (s.op = 1),
  t.end_lsn    = s.lsn
WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT (
  customer_id, title, first_name, middle_name, last_name, suffix,
  company_name, sales_person, email_address, phone, modified_date,
  valid_from, valid_to, is_current, is_deleted, start_lsn, end_lsn
) VALUES (
  s.customer_id, s.title, s.first_name, s.middle_name, s.last_name, s.suffix,
  s.company_name, s.sales_person, s.email_address, s.phone, s.modified_date,
  s.valid_from, s.valid_to, s.is_current, s.is_deleted, s.lsn, s.end_lsn
);