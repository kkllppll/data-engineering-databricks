# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Sync the search index
# MAGIC The index is TRIGGERED, so after new vectors land in the chunks table it has to be told to sync
# MAGIC Through the change data feed it reads only the rows changed since its last sync
# MAGIC Waits until this sync finishes and checks that the index holds every chunk with a vector,
# MAGIC so a job task after this one searches the new data, not the old

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

index_name = f"{catalog}.{schema}.chunks_index"
chunks_name = f"{catalog}.{schema}.chunks"

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import UpdateStateInfoState

POLL_INTERVAL_S = 20
SYNC_TIMEOUT_S = 1800
FINISHED = {
    UpdateStateInfoState.COMPLETED,
    UpdateStateInfoState.FAILED,
    UpdateStateInfoState.CANCELED,
}

w = WorkspaceClient()
pipeline_id = w.vector_search_indexes.get_index(
    index_name
).delta_sync_index_spec.pipeline_id


def pipeline_updates():
    return w.pipelines.get(pipeline_id).latest_updates or []


def wait_until(condition, what):
    deadline = time.monotonic() + SYNC_TIMEOUT_S
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError(f"{what} did not happen within {SYNC_TIMEOUT_S} s")
        time.sleep(POLL_INTERVAL_S)


# COMMAND ----------

# a sync that is still running would make the new trigger fail
wait_until(
    lambda: all(u.state in FINISHED for u in pipeline_updates()),
    "The previous sync finishing",
)

known_updates = {u.update_id for u in pipeline_updates()}
w.vector_search_indexes.sync_index(index_name)


def new_update():
    return next(
        (u for u in pipeline_updates() if u.update_id not in known_updates), None
    )


wait_until(
    lambda: new_update() is not None and new_update().state in FINISHED,
    "This sync finishing",
)

state = new_update().state
if state != UpdateStateInfoState.COMPLETED:
    raise RuntimeError(
        f"Index sync ended with {state.value}, see pipeline {pipeline_id}"
    )

# COMMAND ----------

expected = spark.table(chunks_name).where("embedding IS NOT NULL").count()
indexed = w.vector_search_indexes.get_index(index_name).status.indexed_row_count
if indexed != expected:
    raise RuntimeError(
        f"The index holds {indexed} rows, the chunks table has {expected} chunks with a vector"
    )
