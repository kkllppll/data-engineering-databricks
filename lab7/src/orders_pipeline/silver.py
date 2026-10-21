from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

AMOUNT_TOLERANCE_USD = 0.01

ORDER_RULES = {
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "order_time_not_null": "order_time IS NOT NULL",
    "customer_exists": "_customer_known",
    "subtotal_non_negative": "subtotal_usd >= 0",
    "discount_in_range": "discount_pct BETWEEN 0 AND 100",
    "total_matches_discount": (
        f"abs(total_usd - subtotal_usd * (1 - discount_pct / 100)) <= {AMOUNT_TOLERANCE_USD}"
    ),
}

ORDER_ITEM_RULES = {
    "order_id_not_null": "order_id IS NOT NULL",
    "product_id_not_null": "product_id IS NOT NULL",
    "no_rescued_data": "_rescued_data IS NULL",
    "order_exists": "_order_known",
    "product_exists": "_product_known",
    "quantity_positive": "quantity > 0",
    "unit_price_positive": "unit_price_usd > 0",
    "line_total_matches_price": (
        f"abs(line_total_usd - unit_price_usd * quantity) <= {AMOUNT_TOLERANCE_USD}"
    ),
}


def keep_latest(df: DataFrame, key: str, order_by: str) -> DataFrame:
    latest_first = Window.partitionBy(key).orderBy(F.col(order_by).desc())
    return (
        df.withColumn("_rn", F.row_number().over(latest_first))
        .filter((F.col("_rn") == 1) | F.col(key).isNull())
        .drop("_rn")
    )


def flag_known(df: DataFrame, reference: DataFrame, key: str, flag: str) -> DataFrame:
    known = reference.select(key).distinct().withColumn(flag, F.lit(True))
    return df.join(known, key, "left")


def add_failed_rules(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    failed = [
        F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
        for name, rule in rules.items()
    ]
    return df.withColumn("failed_rules", F.array_compact(F.array(*failed)))


def validate_orders(bronze_orders: DataFrame, customers: DataFrame) -> DataFrame:
    latest = keep_latest(bronze_orders, "order_id", "ingestion_timestamp")
    flagged = flag_known(latest, customers, "customer_id", "_customer_known")
    return add_failed_rules(flagged, ORDER_RULES)


def validate_order_items(
    bronze_items: DataFrame, orders: DataFrame, products: DataFrame
) -> DataFrame:
    flagged = flag_known(bronze_items, orders, "order_id", "_order_known")
    flagged = flag_known(flagged, products, "product_id", "_product_known")
    return add_failed_rules(flagged, ORDER_ITEM_RULES)


def add_line_number(items: DataFrame) -> DataFrame:
    line_order = Window.partitionBy("order_id").orderBy(
        "product_id", "unit_price_usd", "quantity", "line_total_usd"
    )
    return items.withColumn("line_number", F.row_number().over(line_order))


def to_quarantine(
    rejected: DataFrame, source_table: str, record_columns: list[str]
) -> DataFrame:
    return rejected.select(
        F.lit(source_table).alias("source_table"),
        F.to_json(F.struct(*record_columns)).alias("record"),
        "failed_rules",
        F.current_timestamp().alias("quarantined_at"),
    )
