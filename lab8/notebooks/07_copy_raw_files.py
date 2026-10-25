# Databricks notebook source
# Copies the source CSV files from the landing zone into the raw volume of the current environment.
# Only files that are not in the target yet are copied, so the task is safe to run on every job run.
# In DEV the landing zone is the raw volume itself, so there is nothing to copy.

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix")
SOURCE_ROOT = dbutils.widgets.get("raw_source_path").rstrip("/")

TARGET_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_PREFIX}/raw_data"

# Folder of the volume -> file name prefixes that belong to this project.
# events.csv in the same volume belongs to the Lab 3 streaming exercise and is not copied.
SOURCE_FILES = {
    "": ("orders", "customers", "products"),
    "order_items": ("order_items",),
    "reviews": ("reviews",),
}

# COMMAND ----------

if SOURCE_ROOT == TARGET_ROOT:
    dbutils.notebook.exit("The landing zone is the raw volume of this environment: nothing to copy")

# COMMAND ----------

for folder, prefixes in SOURCE_FILES.items():
    source_dir = f"{SOURCE_ROOT}/{folder}".rstrip("/")
    target_dir = f"{TARGET_ROOT}/{folder}".rstrip("/")

    dbutils.fs.mkdirs(target_dir)
    existing = {f.name for f in dbutils.fs.ls(target_dir)}

    for f in dbutils.fs.ls(source_dir):
        if f.name.endswith(".csv") and f.name.startswith(prefixes) and f.name not in existing:
            dbutils.fs.cp(f.path, f"{target_dir}/{f.name}")
