# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Ask the assistant
# MAGIC Answers one question about Saleor: finds the top-k closest chunks (optionally only in one topic),
# MAGIC passes them to the LLM as numbered excerpts and returns the answer with its sources
# MAGIC Every answer is logged to assistant_log. The result is the notebook exit value, so a job run shows it

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text(
    "question", "Can I change the channel of a checkout after it is created?"
)
dbutils.widgets.text("topic", "")
dbutils.widgets.text("top_k", "5")
dbutils.widgets.text("max_tokens", "600")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
question = dbutils.widgets.get("question").strip()
topic = dbutils.widgets.get("topic").strip() or None
top_k = int(dbutils.widgets.get("top_k"))
max_tokens = int(dbutils.widgets.get("max_tokens"))

if not question:
    raise ValueError("The question is empty")

# COMMAND ----------

import json
import os
import sys

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))
from saleor_rag.assistant import Assistant, to_log_row

log_table = f"{catalog}.{schema}.assistant_log"

assistant = Assistant(
    client=WorkspaceClient(),
    embedding_endpoint=embedding_endpoint,
    index_name=f"{catalog}.{schema}.chunks_index",
    llm_endpoint=llm_endpoint,
    max_tokens=max_tokens,
)

# COMMAND ----------

result = assistant.answer(question, top_k=top_k, topic=topic)

asked_by = spark.sql("SELECT current_user()").first()[0]
log_schema = spark.table(log_table).drop("asked_at").schema
(
    spark.createDataFrame([to_log_row(result, asked_by, source="ask")], log_schema)
    .withColumn("asked_at", F.current_timestamp())
    .write.mode("append")
    .saveAsTable(log_table)
)

# COMMAND ----------

dbutils.notebook.exit(
    json.dumps(
        {
            "answer": result.answer,
            "refused": result.refused,
            "sources": [
                {"n": number, "chunk_id": chunk["chunk_id"], "url": chunk["source_url"]}
                for number, chunk in enumerate(result.chunks, start=1)
            ],
        },
        ensure_ascii=False,
    )
)
