from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MONEY = "decimal(12,2)"
UNKNOWN_MEMBER = "Unknown"
SCD2_BEGINNING_OF_TIME = "1900-01-01 00:00:00"
SCD2_END_OF_TIME = "9999-12-31 23:59:59"
CHANNEL_COLUMNS = ["payment_method", "device", "source"]

FACT_COLUMNS = [
    "order_id",
    "line_number",
    "date_key",
    "customer_sk",
    "product_id",
    "country_key",
    "order_channel_key",
    "quantity",
    "unit_price_usd",
    "discount_pct",
    "gross_amount_usd",
    "discount_amount_usd",
    "net_amount_usd",
    "processed_at",
]


def fill_unknown(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.fillna(UNKNOWN_MEMBER, subset=columns)


def add_date_key(df: DataFrame, timestamp_column: str) -> DataFrame:
    return df.withColumn(
        "date_key", F.date_format(timestamp_column, "yyyyMMdd").cast("int")
    )


def build_dim_customer(customers: DataFrame) -> DataFrame:
    first_version = F.min("effective_from").over(Window.partitionBy("customer_id"))
    return customers.select(
        "customer_sk",
        "customer_id",
        F.col("name").alias("customer_name"),
        "email",
        "country",
        "age",
        "loyalty_tier",
        "signup_date",
        "marketing_opt_in",
        F.when(
            F.col("effective_from") == first_version,
            F.to_timestamp(F.lit(SCD2_BEGINNING_OF_TIME)),
        )
        .otherwise(F.col("effective_from"))
        .alias("effective_from"),
        F.coalesce("effective_to", F.to_timestamp(F.lit(SCD2_END_OF_TIME))).alias(
            "effective_to"
        ),
        "is_current",
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        "product_id",
        F.col("name").alias("product_name"),
        F.coalesce("category", F.lit(UNKNOWN_MEMBER)).alias("category"),
        F.col("price_usd").cast(MONEY).alias("price_usd"),
    )


def order_country_codes(orders: DataFrame) -> DataFrame:
    return (
        fill_unknown(orders, ["country"])
        .select(F.col("country").alias("country_code"))
        .distinct()
    )


def order_channels(orders: DataFrame) -> DataFrame:
    return fill_unknown(orders, CHANNEL_COLUMNS).select(*CHANNEL_COLUMNS).distinct()


def allocate_net_amounts(lines: DataFrame) -> DataFrame:
    """Expects order_id, line_number, line_total_usd, discount_pct and order_total_usd."""
    per_order = Window.partitionBy("order_id")
    is_last_line = F.col("line_number") == F.max("line_number").over(per_order)
    residual = F.col("order_total_usd").cast(MONEY) - F.sum("_net_rounded").over(
        per_order
    )
    return (
        lines.withColumn("gross_amount_usd", F.col("line_total_usd").cast(MONEY))
        .withColumn(
            "_net_rounded",
            (F.col("line_total_usd") * (1 - F.col("discount_pct") / 100)).cast(MONEY),
        )
        .withColumn(
            "net_amount_usd",
            (F.col("_net_rounded") + F.when(is_last_line, residual).otherwise(0)).cast(
                MONEY
            ),
        )
        .withColumn(
            "discount_amount_usd",
            (F.col("gross_amount_usd") - F.col("net_amount_usd")).cast(MONEY),
        )
        .drop("_net_rounded")
    )


def attach_customer_version(lines: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Adds customer_sk of the customer version valid at order_time; NULL if none is valid."""
    versions = dim_customer.select(
        F.col("customer_id").alias("_version_customer_id"),
        "customer_sk",
        F.col("effective_from").alias("_valid_from"),
        F.col("effective_to").alias("_valid_to"),
    )
    is_valid_version = (F.col("customer_id") == F.col("_version_customer_id")) & F.col(
        "order_time"
    ).between(F.col("_valid_from"), F.col("_valid_to"))
    return lines.join(versions, is_valid_version, "left").drop(
        "_version_customer_id", "_valid_from", "_valid_to"
    )


def build_fact_order_items(
    order_items: DataFrame,
    orders: DataFrame,
    dim_customer: DataFrame,
    dim_country: DataFrame,
    dim_order_channel: DataFrame,
) -> DataFrame:
    order_attributes = fill_unknown(orders, ["country", *CHANNEL_COLUMNS]).select(
        "order_id",
        "customer_id",
        "order_time",
        "discount_pct",
        F.col("total_usd").alias("order_total_usd"),
        F.col("country").alias("country_code"),
        *CHANNEL_COLUMNS,
    )
    lines = order_items.select(
        "order_id",
        "line_number",
        "product_id",
        "quantity",
        "unit_price_usd",
        "line_total_usd",
    ).join(order_attributes, "order_id")

    lines = allocate_net_amounts(lines)
    lines = attach_customer_version(lines, dim_customer)
    lines = lines.join(
        dim_country.select("country_code", "country_key"), "country_code", "left"
    )
    lines = lines.join(
        dim_order_channel.select(*CHANNEL_COLUMNS, "order_channel_key"),
        CHANNEL_COLUMNS,
        "left",
    )
    return (
        add_date_key(lines, "order_time")
        .withColumn("unit_price_usd", F.col("unit_price_usd").cast(MONEY))
        .withColumn("processed_at", F.current_timestamp())
        .select(*FACT_COLUMNS)
    )
