# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 12: Ingest the Saleor docs
# MAGIC Downloads the selected pages of saleor/saleor-docs at a pinned commit into the raw_docs volume and syncs the documents table.
# MAGIC Files already in the volume are not downloaded again, so a rerun only reads them.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "elina_sharabura_lab12")
dbutils.widgets.text("docs_repo", "saleor/saleor-docs")
dbutils.widgets.text("docs_commit", "1ba3c4359962ce9bdfef0231d067d10dc9b859de")
dbutils.widgets.text("docs_root", "docs/developer")
dbutils.widgets.text(
    "topics",
    "address,attributes,channels,checkout,discounts,gift-cards,order,payments,"
    "permissions,price-calculation,price-freeze-period,products,shipping,stock,taxes,users",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
docs_repo = dbutils.widgets.get("docs_repo")
docs_commit = dbutils.widgets.get("docs_commit")
docs_root = dbutils.widgets.get("docs_root").strip("/")
topics = {t.strip() for t in dbutils.widgets.get("topics").split(",") if t.strip()}

# COMMAND ----------

import json
import os
import sys
import urllib.request

from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../src"))
from saleor_rag.mdx import page_title

HTTP_TIMEOUT_S = 60
DOC_EXTENSIONS = (".md", ".mdx")


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_S) as response:
        return response.read()


# COMMAND ----------

tree = json.loads(
    fetch(
        f"https://api.github.com/repos/{docs_repo}/git/trees/{docs_commit}?recursive=1"
    )
)
if tree.get("truncated"):
    raise RuntimeError(
        "GitHub returned a truncated file tree, the page list would be incomplete"
    )

prefix = f"{docs_root}/"
pages = []
for item in tree["tree"]:
    path = item["path"]
    if (
        item["type"] != "blob"
        or not path.startswith(prefix)
        or not path.endswith(DOC_EXTENSIONS)
    ):
        continue
    relative = path[len(prefix) :]
    topic = os.path.splitext(relative.split("/")[0])[0]
    if topic in topics:
        pages.append((path, relative, topic))

missing = topics - {topic for _, _, topic in pages}
if missing:
    raise ValueError(f"Topics not found at commit {docs_commit}: {sorted(missing)}")

# COMMAND ----------

landing = (
    f"/Volumes/{catalog}/{schema}/raw_docs/{docs_repo.replace('/', '__')}/{docs_commit}"
)

rows = []
for path, relative, topic in pages:
    local = f"{landing}/{relative}"
    if not os.path.exists(local):
        os.makedirs(os.path.dirname(local), exist_ok=True)
        # write under a temp name first, so a broken download is never taken for a finished file
        with open(f"{local}.part", "wb") as f:
            f.write(
                fetch(
                    f"https://raw.githubusercontent.com/{docs_repo}/{docs_commit}/{path}"
                )
            )
        os.replace(f"{local}.part", local)

    with open(local, encoding="utf-8") as f:
        raw_text = f.read()

    rows.append(
        {
            "doc_id": os.path.splitext(relative)[0],
            "topic": topic,
            "title": page_title(raw_text),
            "raw_text": raw_text,
            "source_url": f"https://github.com/{docs_repo}/blob/{docs_commit}/{path}",
            "source_commit": docs_commit,
        }
    )

# COMMAND ----------

documents = DeltaTable.forName(spark, f"{catalog}.{schema}.documents")
source = spark.createDataFrame(
    rows, documents.toDF().drop("ingested_at").schema
).withColumn("ingested_at", F.current_timestamp())

(
    documents.alias("t")
    .merge(source.alias("s"), "t.doc_id = s.doc_id")
    .whenMatchedUpdateAll(
        condition="t.raw_text <> s.raw_text OR t.source_commit <> s.source_commit"
    )
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)
