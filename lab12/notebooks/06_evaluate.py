# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Evaluate the assistant
# MAGIC Answers every question of the eval set twice, with retrieval (rag) and without it (no_retrieval), with the same LLM
# MAGIC A judge model from another family scores each answer against the reference: correctness, relevance and, for rag, groundedness
# MAGIC Also records where the expected page was in the top-k (retrieval quality) and whether the assistant refused (hallucination risk, missing context).
# MAGIC The answers go to assistant_log (source = eval), the scores to eval_runs, both under one run_id

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text("judge_endpoint", "databricks-gpt-oss-120b")
dbutils.widgets.text("top_k", "5")
dbutils.widgets.text("max_tokens", "600")
dbutils.widgets.text("judge_max_tokens", "2000")
dbutils.widgets.text("eval_set_path", "../eval/eval_set.json")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
judge_endpoint = dbutils.widgets.get("judge_endpoint")
top_k = int(dbutils.widgets.get("top_k"))
max_tokens = int(dbutils.widgets.get("max_tokens"))
judge_max_tokens = int(dbutils.widgets.get("judge_max_tokens"))
eval_set_path = dbutils.widgets.get("eval_set_path")

# COMMAND ----------

import json
import os
import sys
import uuid

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))
from saleor_rag.assistant import Assistant, to_log_row
from saleor_rag.judge import (
    build_judge_messages,
    content_text,
    expected_doc_rank,
    parse_verdict,
)
from saleor_rag.prompts import format_excerpts

MODES = ["rag", "no_retrieval"]
# a reasoning model sometimes adds text around the JSON, one more try is cheaper than losing the run
JUDGE_ATTEMPTS = 2

eval_set_table = f"{catalog}.{schema}.eval_set"
eval_runs_table = f"{catalog}.{schema}.eval_runs"
log_table = f"{catalog}.{schema}.assistant_log"

client = WorkspaceClient()
assistant = Assistant(
    client=client,
    embedding_endpoint=embedding_endpoint,
    index_name=f"{catalog}.{schema}.chunks_index",
    llm_endpoint=llm_endpoint,
    max_tokens=max_tokens,
)

# COMMAND ----------

with open(eval_set_path, encoding="utf-8") as f:
    questions = json.load(f)

eval_set = DeltaTable.forName(spark, eval_set_table)
source = spark.createDataFrame(
    questions, eval_set.toDF().drop("updated_at").schema
).withColumn("updated_at", F.current_timestamp())
(
    eval_set.alias("t")
    .merge(source.alias("s"), "t.question_id = s.question_id")
    .whenMatchedUpdateAll(
        condition="NOT (t.question <=> s.question AND t.reference_answer <=> s.reference_answer "
        "AND t.answerable <=> s.answerable AND t.expected_doc_id <=> s.expected_doc_id)"
    )
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)

# COMMAND ----------


def judge(question, reference, answer, chunks, refused):
    excerpts = format_excerpts(chunks) if chunks else None
    messages = build_judge_messages(question, reference, answer, excerpts)
    for attempt in range(1, JUDGE_ATTEMPTS + 1):
        response = client.serving_endpoints.query(
            name=judge_endpoint,
            messages=[
                ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"])
                for m in messages
            ],
            temperature=0.0,
            max_tokens=judge_max_tokens,
        )
        try:
            # a refusal makes no claims, so there is nothing to be grounded and the score would only be noise
            return parse_verdict(
                content_text(response.choices[0].message.content),
                has_excerpts=bool(chunks) and not refused,
            )
        except ValueError:
            if attempt == JUDGE_ATTEMPTS:
                raise


run_id = str(uuid.uuid4())
asked_by = spark.sql("SELECT current_user()").first()[0]

log_rows = []
eval_rows = []
for q in spark.table(eval_set_table).orderBy("question_id").collect():
    for mode in MODES:
        result = assistant.answer(
            q.question, top_k=top_k, use_retrieval=(mode == "rag")
        )
        verdict = judge(
            q.question, q.reference_answer, result.answer, result.chunks, result.refused
        )
        log_rows.append(to_log_row(result, asked_by, source="eval"))
        eval_rows.append(
            {
                "run_id": run_id,
                "question_id": q.question_id,
                "mode": mode,
                "request_id": result.request_id,
                "answerable": q.answerable,
                "refused": result.refused,
                # answering an unanswerable question is a hallucination, refusing an answerable one is missing context
                "behavior_correct": q.answerable != result.refused,
                "expected_doc_rank": expected_doc_rank(
                    [c["doc_id"] for c in result.chunks], q.expected_doc_id
                ),
                "correctness": verdict.correctness,
                "relevance": verdict.relevance,
                "groundedness": verdict.groundedness,
                "judge_reason": verdict.reason,
                "judge_endpoint": judge_endpoint,
            }
        )

# COMMAND ----------

# written only after every question is answered and judged, so a failed run leaves no half results
(
    spark.createDataFrame(log_rows, spark.table(log_table).drop("asked_at").schema)
    .withColumn("asked_at", F.current_timestamp())
    .write.mode("append")
    .saveAsTable(log_table)
)
(
    spark.createDataFrame(
        eval_rows, spark.table(eval_runs_table).drop("evaluated_at").schema
    )
    .withColumn("evaluated_at", F.current_timestamp())
    .write.mode("append")
    .saveAsTable(eval_runs_table)
)

dbutils.notebook.exit(run_id)
