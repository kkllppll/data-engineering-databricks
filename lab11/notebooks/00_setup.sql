-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 11: Setup (DDL)
-- MAGIC Schema, Zerobus target table, silver table and checkpoint volume, plus grants for the Zerobus service principal.
-- MAGIC Source: e-commerce clickstream events (events.csv), replayed by a local producer as if they came from the shop backend.
-- MAGIC Run once manually, not part of any job.
-- MAGIC Zerobus can't write to a table that was dropped or recreated, so nothing here uses CREATE OR REPLACE.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dbr_dev_ua5816bd.elina_sharabura_lab11
COMMENT 'Lab 11: shop clickstream events pushed directly through Zerobus Ingest';

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS dbr_dev_ua5816bd.elina_sharabura_lab11.bronze_events (
  event_id          BIGINT    NOT NULL COMMENT 'unique in the source, the same for every resend',
  session_id        BIGINT,
  event_ts          TIMESTAMP          COMMENT 'when the event happened in the shop',
  event_type        STRING,
  product_id        DOUBLE,
  qty               DOUBLE,
  cart_size         DOUBLE,
  payment           STRING,
  discount_pct      DOUBLE,
  amount_usd        DOUBLE,
  raw_payload       VARIANT            COMMENT 'source csv row as received',
  producer_id       STRING    NOT NULL COMMENT 'producer run that sent the record',
  producer_seq      BIGINT    NOT NULL COMMENT 'counter inside one producer run, a gap means a lost record',
  producer_sent_at  TIMESTAMP NOT NULL COMMENT 'when the producer sent the record'
)
CLUSTER BY (producer_sent_at)
COMMENT 'Raw shop events from Zerobus Ingest, may contain duplicates';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dbr_dev_ua5816bd.elina_sharabura_lab11.silver_events (
  event_id          BIGINT        NOT NULL,
  session_id        BIGINT,
  event_ts          TIMESTAMP,
  event_type        STRING,
  product_id        BIGINT,
  qty               INT,
  cart_size         INT,
  payment           STRING,
  discount_pct      DOUBLE,
  amount_usd        DECIMAL(12,2),
  producer_sent_at  TIMESTAMP     NOT NULL,
  processed_at      TIMESTAMP     NOT NULL COMMENT 'when the silver job inserted the row'
)
CLUSTER BY (producer_sent_at)
COMMENT 'Shop events without duplicates';

-- COMMAND ----------

-- Checkpoint of the bronze to silver stream
CREATE VOLUME IF NOT EXISTS dbr_dev_ua5816bd.elina_sharabura_lab11.checkpoints
COMMENT 'Structured Streaming checkpoints for Lab 11';

-- COMMAND ----------

-- Zerobus producer identity (elina-sharabura-zerobus-sp): write access to the bronze table only
GRANT USE CATALOG ON CATALOG dbr_dev_ua5816bd TO `311eb24f-62b3-4984-8268-966790ccd231`;
GRANT USE SCHEMA ON SCHEMA dbr_dev_ua5816bd.elina_sharabura_lab11 TO `311eb24f-62b3-4984-8268-966790ccd231`;
GRANT MODIFY, SELECT ON TABLE dbr_dev_ua5816bd.elina_sharabura_lab11.bronze_events
  TO `311eb24f-62b3-4984-8268-966790ccd231`;