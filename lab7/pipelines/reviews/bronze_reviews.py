from pyspark import pipelines as dp
from pyspark.sql import functions as F

REVIEWS_SOURCE_PATH = spark.conf.get("reviews.source_path")


@dp.table(
    name="reviews_v2",
    comment="Raw product reviews ingested incrementally from CSV files with Auto Loader",
    table_properties={"delta.appendOnly": "false"},
)
def reviews_v2():
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
