from pyspark import pipelines as dp
from pyspark.sql import functions as F

REVIEWS_SOURCE_PATH = spark.conf.get("reviews.source_path")


@dp.table(
    name="reviews",
    comment="Raw product reviews ingested incrementally from CSV files with Auto Loader",
    table_properties={"delta.appendOnly": "true"},
)
def reviews():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("escape", '"')
        # unknown columns in future files land in _rescued_data instead of stopping the pipeline
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(REVIEWS_SOURCE_PATH)
        .withColumn("source_filename", F.col("_metadata.file_name"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
