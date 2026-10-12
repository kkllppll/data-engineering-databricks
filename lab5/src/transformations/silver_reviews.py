from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SCHEMA = spark.conf.get("silver.schema")


REVIEW_DROP_RULES = {
    "valid_rating": "rating IS NOT NULL AND rating BETWEEN 1 AND 5",
    "has_order_and_product": "order_id IS NOT NULL AND product_id IS NOT NULL",
    "valid_review_time": "review_time IS NOT NULL",
}

QUARANTINE_CONDITION = "NOT coalesce({}, false)".format(
    " AND ".join(f"({rule})" for rule in REVIEW_DROP_RULES.values())
)


@dp.temporary_view(name="reviews_typed")
def reviews_typed():
    return spark.read.table("reviews").select(
        F.expr("try_cast(review_id AS BIGINT)").alias("review_id"),
        F.expr("try_cast(order_id AS BIGINT)").alias("order_id"),
        F.expr("try_cast(product_id AS BIGINT)").alias("product_id"),
        F.expr("try_cast(rating AS INT)").alias("rating"),
        F.trim("review_text").alias("review_text"),
        F.expr("try_cast(review_time AS TIMESTAMP)").alias("review_time"),
        "source_filename",
        "ingestion_timestamp",
    )


@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.reviews",
    comment="Validated product reviews with typed columns",
)
@dp.expect_or_fail("valid_review_id", "review_id IS NOT NULL")
@dp.expect_all_or_drop(REVIEW_DROP_RULES)
@dp.expect("has_review_text", "review_text IS NOT NULL AND review_text <> ''")
def silver_reviews():
    return spark.read.table("reviews_typed")


@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.reviews_quarantine",
    comment="Reviews rejected by silver data-quality rules, kept for investigation",
)
def reviews_quarantine():
    return spark.read.table("reviews_typed").where(QUARANTINE_CONDITION)
