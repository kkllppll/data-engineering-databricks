from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SCHEMA = spark.conf.get("silver.schema")
REVIEWS_TABLE = spark.conf.get("reviews.table")

# a missing primary key means a broken file, not a bad row, so the update stops
FAIL_RULES = {
    "valid_review_id": "review_id IS NOT NULL",
}
DROP_RULES = {
    "valid_rating": "rating IS NOT NULL AND rating BETWEEN 1 AND 5",
    "has_order_and_product": "order_id IS NOT NULL AND product_id IS NOT NULL",
    "valid_review_time": "review_time IS NOT NULL",
}
WARN_RULES = {
    "has_review_text": "review_text IS NOT NULL AND review_text <> ''",
}


def failed_rule_names(rules: dict[str, str]):
    return F.array_compact(
        F.array(*[F.when(~F.expr(rule), F.lit(name)) for name, rule in rules.items()])
    )


@dp.temporary_view(name="reviews_typed")
def reviews_typed():
    return spark.read.table(REVIEWS_TABLE).select(
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
    name=f"{SILVER_SCHEMA}.{REVIEWS_TABLE}",
    comment="Validated product reviews with typed columns",
)
@dp.expect_all_or_fail(FAIL_RULES)
@dp.expect_all_or_drop(DROP_RULES)
@dp.expect_all(WARN_RULES)
def silver_reviews():
    return spark.read.table("reviews_typed")


@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.{REVIEWS_TABLE}_quarantine",
    comment="Reviews rejected by the silver drop rules, with the rules each row breaks",
)
def reviews_quarantine():
    return (
        spark.read.table("reviews_typed")
        .withColumn("failed_rules", failed_rule_names(DROP_RULES))
        .filter(F.size("failed_rules") > 0)
        .withColumn("quarantined_at", F.current_timestamp())
    )
