# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Setup (DDL)
# MAGIC Schema, volume for the raw Saleor docs and the tables of the RAG pipeline.
# MAGIC Source: Saleor developer docs (github.com/saleor/saleor-docs, BSD-3-Clause), pinned to one commit.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq_schema = f"{catalog}.{schema}"

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {fq_schema}
COMMENT 'Lab 12: RAG knowledge assistant over the Saleor developer docs'
""")

# COMMAND ----------

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {fq_schema}.raw_docs
COMMENT 'Saleor docs .mdx files as downloaded, one folder per source commit'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq_schema}.documents (
  doc_id         STRING    NOT NULL COMMENT 'path inside docs/developer without extension, e.g. checkout/lifecycle',
  topic          STRING    NOT NULL COMMENT 'top folder of the page, e.g. checkout, used as a search filter',
  title          STRING             COMMENT 'title from the page front matter',
  raw_text       STRING    NOT NULL COMMENT 'page as downloaded, front matter included',
  source_url     STRING    NOT NULL COMMENT 'page on GitHub at the source commit',
  source_commit  STRING    NOT NULL COMMENT 'saleor-docs commit the page was taken from',
  ingested_at    TIMESTAMP NOT NULL,
  CONSTRAINT documents_pk PRIMARY KEY (doc_id)
)
COMMENT 'One row per Saleor docs page'
""")

# COMMAND ----------

# CDF lets the Delta Sync index pick up only changed chunks
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq_schema}.chunks (
  chunk_id         STRING       NOT NULL COMMENT 'doc_id#chunk_index, the same between runs',
  doc_id           STRING       NOT NULL,
  chunk_index      INT          NOT NULL COMMENT 'position of the chunk in the page',
  topic            STRING       NOT NULL,
  title            STRING,
  section          STRING                COMMENT 'heading path inside the page',
  chunk_text       STRING       NOT NULL COMMENT 'cleaned text with the title and section on top',
  char_count       INT          NOT NULL,
  content_hash     STRING       NOT NULL COMMENT 'sha2 of chunk_text, a new hash means the vector is stale',
  source_url       STRING       NOT NULL,
  source_commit    STRING       NOT NULL,
  embedding        ARRAY<FLOAT>          COMMENT 'null until the embed step runs',
  embedding_model  STRING                COMMENT 'serving endpoint that produced the vector',
  embedded_at      TIMESTAMP,
  updated_at       TIMESTAMP    NOT NULL,
  CONSTRAINT chunks_pk PRIMARY KEY (chunk_id)
)
COMMENT 'Chunks of the Saleor docs with their vectors, source of the vector search index'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


# COMMAND ----------

# holds the questions user ask, which may contain their data, so only the owner reads it
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq_schema}.assistant_log (
  request_id           STRING        NOT NULL,
  asked_at             TIMESTAMP     NOT NULL,
  asked_by             STRING        NOT NULL COMMENT 'user or job identity that asked',
  source               STRING        NOT NULL COMMENT 'ask: a person, eval: an evaluation run',
  mode                 STRING        NOT NULL COMMENT 'rag or no_retrieval',
  question             STRING        NOT NULL,
  topic_filter         STRING,
  top_k                INT,
  retrieved_chunk_ids  ARRAY<STRING>,
  retrieved_scores     ARRAY<DOUBLE>,
  prompt               STRING        NOT NULL COMMENT 'messages sent to the LLM, as JSON',
  answer               STRING,
  refused              BOOLEAN                COMMENT 'the answer is the fixed refusal phrase',
  llm_endpoint         STRING        NOT NULL,
  prompt_tokens        INT,
  completion_tokens    INT,
  latency_ms           INT,
  error                STRING,
  CONSTRAINT assistant_log_pk PRIMARY KEY (request_id)
)
COMMENT 'Every question to the assistant with what it retrieved and answered'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq_schema}.eval_set (
  question_id       STRING     NOT NULL,
  question          STRING     NOT NULL,
  reference_answer  STRING     NOT NULL COMMENT 'written from the documentation page',
  answerable        BOOLEAN    NOT NULL COMMENT 'false: the corpus does not cover it, the right answer is a refusal',
  expected_doc_id   STRING              COMMENT 'page that holds the answer, null when not answerable',
  updated_at        TIMESTAMP  NOT NULL,
  CONSTRAINT eval_set_pk PRIMARY KEY (question_id)
)
COMMENT 'Evaluation questions with reference answers, a copy of eval/eval_set.json in the repo'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq_schema}.eval_runs (
  run_id             STRING     NOT NULL,
  evaluated_at       TIMESTAMP  NOT NULL,
  question_id        STRING     NOT NULL,
  mode               STRING     NOT NULL COMMENT 'rag or no_retrieval',
  request_id         STRING     NOT NULL COMMENT 'the answer in assistant_log',
  answerable         BOOLEAN    NOT NULL,
  refused            BOOLEAN    NOT NULL,
  behavior_correct   BOOLEAN    NOT NULL COMMENT 'answered an answerable question or refused an unanswerable one',
  expected_doc_rank  INT                 COMMENT 'position of the expected page in the top-k, null when not found',
  correctness        INT        NOT NULL COMMENT '1-5 from the judge, against the reference answer',
  relevance          INT        NOT NULL COMMENT '1-5 from the judge',
  groundedness       INT                 COMMENT '1-5 from the judge, null without retrieval',
  judge_reason       STRING,
  judge_endpoint     STRING     NOT NULL,
  CONSTRAINT eval_runs_pk PRIMARY KEY (run_id, question_id, mode)
)
COMMENT 'Scores of every evaluation run, one row per question and mode'
""")
