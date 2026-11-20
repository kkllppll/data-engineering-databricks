# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Embed the chunks
# MAGIC Computes a vector for every chunk that has none yet (new chunks, or chunks whose text changed) with ai_query
# MAGIC and stores it in the chunks table, next to the text, with the endpoint name and the time.
# MAGIC Self-managed embeddings: the vectors are our data in Unity Catalog, so the search index can be rebuilt from them without paying for embeddings again.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")
dbutils.widgets.text("embedding_dim", "1024")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
embedding_dim = int(dbutils.widgets.get("embedding_dim"))

chunks_name = f"{catalog}.{schema}.chunks"

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

# the vectors are collected once, so the model is called once per chunk;
# a large corpus would write them to a staging table instead of the driver
embedded = (
    spark.table(chunks_name)
    .where("embedding IS NULL")
    .select(
        "chunk_id",
        "content_hash",
        F.call_function(
            "ai_query", F.lit(embedding_endpoint), F.col("chunk_text")
        ).alias("embedding"),
    )
    .collect()
)

if not embedded:
    dbutils.notebook.exit("All chunks already have a vector")

wrong_size = [
    row.chunk_id
    for row in embedded
    if row.embedding is None or len(row.embedding) != embedding_dim
]
if wrong_size:
    raise ValueError(
        f"{len(wrong_size)} vectors don't have {embedding_dim} dimensions, for example {wrong_size[:5]}"
    )

# COMMAND ----------

source = (
    spark.createDataFrame(
        [(row.chunk_id, row.content_hash, list(row.embedding)) for row in embedded],
        "chunk_id STRING, content_hash STRING, embedding ARRAY<FLOAT>",
    )
    .withColumn("embedding_model", F.lit(embedding_endpoint))
    .withColumn("embedded_at", F.current_timestamp())
)

(
    DeltaTable.forName(spark, chunks_name)
    .alias("t")
    # the hash check skips a chunk whose text changed while its vector was being computed
    .merge(
        source.alias("s"), "t.chunk_id = s.chunk_id AND t.content_hash = s.content_hash"
    )
    .whenMatchedUpdate(
        set={
            "embedding": "s.embedding",
            "embedding_model": "s.embedding_model",
            "embedded_at": "s.embedded_at",
        }
    )
    .execute()
)
