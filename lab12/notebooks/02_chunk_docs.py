# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Clean and chunk the pages
# MAGIC Cleans the MDX of every page in documents, splits it by headings and packs whole paragraphs into chunks with overlap.
# MAGIC Syncs the chunks table with MERGE: a chunk whose text changed loses its vector, so the embed step computes it again.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")
dbutils.widgets.text("max_chars", "1500")
dbutils.widgets.text("overlap_chars", "200")
dbutils.widgets.text("min_chars", "30")
dbutils.widgets.text("drop_code_languages", "json,jsom,mermaid")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
max_chars = int(dbutils.widgets.get("max_chars"))
overlap_chars = int(dbutils.widgets.get("overlap_chars"))
min_chars = int(dbutils.widgets.get("min_chars"))
drop_code_languages = frozenset(
    lang.strip().lower()
    for lang in dbutils.widgets.get("drop_code_languages").split(",")
    if lang.strip()
)

# COMMAND ----------

import hashlib
import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))
from saleor_rag.chunking import chunk_page
from saleor_rag.mdx import clean_body, split_front_matter

# COMMAND ----------

# 66 pages fit on the driver, a large corpus would be chunked with a pandas UDF instead
documents = (
    spark.table(f"{catalog}.{schema}.documents")
    .select("doc_id", "topic", "title", "raw_text", "source_url", "source_commit")
    .collect()
)

rows = []
for doc in documents:
    _, body = split_front_matter(doc.raw_text)
    chunks = chunk_page(
        doc.title or doc.doc_id,
        clean_body(body, drop_code_languages),
        max_chars,
        overlap_chars,
        min_chars,
    )
    for index, chunk in enumerate(chunks):
        rows.append(
            {
                "chunk_id": f"{doc.doc_id}#{index:04d}",
                "doc_id": doc.doc_id,
                "chunk_index": index,
                "topic": doc.topic,
                "title": doc.title,
                "section": chunk.section or None,
                "chunk_text": chunk.text,
                "char_count": len(chunk.text),
                "content_hash": hashlib.sha256(chunk.text.encode("utf-8")).hexdigest(),
                "source_url": doc.source_url,
                "source_commit": doc.source_commit,
            }
        )

if not rows:
    raise ValueError(
        "No chunks were produced, check the documents table and the chunking parameters"
    )

# COMMAND ----------

chunks_table = DeltaTable.forName(spark, f"{catalog}.{schema}.chunks")
source_schema = (
    chunks_table.toDF()
    .drop("embedding", "embedding_model", "embedded_at", "updated_at")
    .schema
)
source = spark.createDataFrame(rows, source_schema).withColumn(
    "updated_at", F.current_timestamp()
)

chunk_columns = [field.name for field in source.schema]
copy_from_source = {column: f"s.{column}" for column in chunk_columns}

(
    chunks_table.alias("t")
    .merge(source.alias("s"), "t.chunk_id = s.chunk_id")
    # new text means the old vector no longer describes it
    .whenMatchedUpdate(
        condition="t.content_hash <> s.content_hash",
        set={
            **copy_from_source,
            "embedding": "NULL",
            "embedding_model": "NULL",
            "embedded_at": "NULL",
        },
    )
    # same text from a newer commit, the vector is still valid
    .whenMatchedUpdate(
        condition="t.source_commit <> s.source_commit",
        set={
            "source_url": "s.source_url",
            "source_commit": "s.source_commit",
            "updated_at": "s.updated_at",
        },
    )
    .whenNotMatchedInsert(values=copy_from_source)
    .whenNotMatchedBySourceDelete()
    .execute()
)
