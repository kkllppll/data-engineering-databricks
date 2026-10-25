from pyspark import pipelines as dp
from pyspark.sql import functions as F

REVIEWS_SOURCE_PATH = spark.conf.get("reviews.source_path")
REVIEWS_TABLE = spark.conf.get("reviews.table")


@dp.table(
    name=REVIEWS_TABLE,
    comment="Raw product reviews ingested incrementally from CSV files with Auto Loader",
    table_properties={"delta.appendOnly": "false"},
)
def bronze_reviews():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("escape", '"')
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(REVIEWS_SOURCE_PATH)
        .withColumn("source_filename", F.col("_metadata.file_name"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
