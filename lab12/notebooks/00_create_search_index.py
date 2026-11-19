# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Setup of the search (AI Search, formerly Vector Search)
# MAGIC Creates the search endpoint and a Delta Sync index on the chunks table
# MAGIC The index reads our own vectors from the embedding column (self-managed embeddings) and is synced on demand (TRIGGERED)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")
dbutils.widgets.text("search_endpoint", "elina-sharabura-lab12")
dbutils.widgets.text("embedding_dim", "1024")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
search_endpoint = dbutils.widgets.get("search_endpoint")
embedding_dim = int(dbutils.widgets.get("embedding_dim"))

source_table = f"{catalog}.{schema}.chunks"
index_name = f"{catalog}.{schema}.chunks_index"

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingVectorColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

# what a search result can return and filter on, the vector column is added by the index itself
SYNCED_COLUMNS = [
    "chunk_id",
    "doc_id",
    "topic",
    "title",
    "section",
    "chunk_text",
    "source_url",
]
POLL_INTERVAL_S = 30
INDEX_READY_TIMEOUT_S = 1800

w = WorkspaceClient()

# COMMAND ----------


try:
    w.vector_search_endpoints.create_endpoint(
        name=search_endpoint, endpoint_type=EndpointType.STANDARD
    )
except (AlreadyExists, ResourceAlreadyExists):
    pass
w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(
    search_endpoint
)

# COMMAND ----------

try:
    w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=search_endpoint,
        primary_key="chunk_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=source_table,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_vector_columns=[
                EmbeddingVectorColumn(
                    name="embedding", embedding_dimension=embedding_dim
                )
            ],
            columns_to_sync=SYNCED_COLUMNS,
        ),
    )
except (AlreadyExists, ResourceAlreadyExists):
    pass

# COMMAND ----------

deadline = time.monotonic() + INDEX_READY_TIMEOUT_S
while True:
    status = w.vector_search_indexes.get_index(index_name).status
    if status and status.ready:
        break
    if time.monotonic() > deadline:
        raise TimeoutError(
            f"Index {index_name} is not ready after {INDEX_READY_TIMEOUT_S} s: {status}"
        )
    time.sleep(POLL_INTERVAL_S)
