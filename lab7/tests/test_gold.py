from datetime import datetime
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from orders_pipeline.gold import (
    add_date_key,
    allocate_net_amounts,
    attach_customer_version,
    build_dim_customer,
    build_fact_order_items,
)

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def cast_to_timestamp(df: DataFrame, *columns: str) -> DataFrame:
    # timestamps are passed as strings: python on windows cannot convert dates before 1970
    # or far in the future to local time, and strings keep tests independent of the each machine's timezone.
    return df.select(
        *[F.col(c).cast("timestamp") if c in columns else c for c in df.columns]
    )


def test_add_date_key_format(spark):
    df = spark.createDataFrame(
        [(datetime(2026, 3, 14, 23, 59, 59),)], "order_time TIMESTAMP"
    )

    assert add_date_key(df, "order_time").first().date_key == 20260314


def test_allocate_net_amounts_residual_to_last_line(spark):
    # each line nets 0.025, which rounds up to 0.03; three lines would sum to 0.09
    # wwhile the order total is 0.08, so the last line absorbs the -0.01.
    lines = spark.createDataFrame(
        [(1, line, 0.05, 50, 0.08) for line in (1, 2, 3)],
        "order_id INT, line_number INT, line_total_usd DOUBLE, discount_pct INT, order_total_usd DOUBLE",
    )

    rows = sorted(
        allocate_net_amounts(lines).collect(), key=lambda row: row.line_number
    )

    assert [row.net_amount_usd for row in rows] == [
        Decimal("0.03"),
        Decimal("0.03"),
        Decimal("0.02"),
    ]
    assert sum(row.net_amount_usd for row in rows) == Decimal("0.08")
    assert all(
        row.gross_amount_usd - row.net_amount_usd == row.discount_amount_usd
        for row in rows
    )


def test_attach_customer_version_by_order_time(spark):
    dim_customer = cast_to_timestamp(
        spark.createDataFrame(
            [
                (100, 1, "1900-01-01 00:00:00", "2026-01-31 23:59:59"),
                (101, 1, "2026-02-01 00:00:00", "9999-12-31 23:59:59"),
            ],
            "customer_sk BIGINT, customer_id INT, effective_from STRING, effective_to STRING",
        ),
        "effective_from",
        "effective_to",
    )
    lines = cast_to_timestamp(
        spark.createDataFrame(
            [
                (1, 1, "2026-01-15 00:00:00"),
                (2, 1, "2026-03-01 00:00:00"),
                (3, 2, "2026-03-01 00:00:00"),
            ],
            "order_id INT, customer_id INT, order_time STRING",
        ),
        "order_time",
    )

    result = {
        row.order_id: row.customer_sk
        for row in attach_customer_version(lines, dim_customer)
        .select("order_id", "customer_sk")
        .collect()
    }

    assert result == {1: 100, 2: 101, 3: None}


def test_build_dim_customer_scd2_boundaries(spark):
    customers = cast_to_timestamp(
        spark.createDataFrame(
            [
                (100, 1, "Ann", "2025-05-01 00:00:00", "2026-01-31 23:59:59", False),
                (101, 1, "Ann", "2026-02-01 00:00:00", None, True),
            ],
            "customer_sk BIGINT, customer_id INT, name STRING, "
            "effective_from STRING, effective_to STRING, is_current BOOLEAN",
        ),
        "effective_from",
        "effective_to",
    ).selectExpr(
        "*",
        "CAST(NULL AS STRING) AS email",
        "CAST(NULL AS STRING) AS country",
        "CAST(NULL AS INT) AS age",
        "CAST(NULL AS STRING) AS loyalty_tier",
        "CAST(NULL AS DATE) AS signup_date",
        "CAST(NULL AS BOOLEAN) AS marketing_opt_in",
    )

    result = {
        row.customer_sk: (row.effective_from, row.effective_to)
        for row in build_dim_customer(customers)
        .select(
            "customer_sk",
            F.date_format("effective_from", TIMESTAMP_FORMAT).alias("effective_from"),
            F.date_format("effective_to", TIMESTAMP_FORMAT).alias("effective_to"),
        )
        .collect()
    }

    assert result == {
        100: ("1900-01-01 00:00:00", "2026-01-31 23:59:59"),
        101: ("2026-02-01 00:00:00", "9999-12-31 23:59:59"),
    }


def test_build_fact_order_items_resolves_dimension_keys(spark):
    order_items = spark.createDataFrame(
        [(10, 1, 5, 1, 10.0, 10.0), (10, 2, 6, 2, 4.0, 8.0)],
        "order_id INT, line_number INT, product_id INT, quantity INT, "
        "unit_price_usd DOUBLE, line_total_usd DOUBLE",
    )
    orders = cast_to_timestamp(
        spark.createDataFrame(
            [
                (
                    10,
                    1,
                    "2026-03-14 10:00:00",
                    10,
                    16.2,
                    None,
                    "card",
                    "mobile",
                    "organic",
                )
            ],
            "order_id INT, customer_id INT, order_time STRING, discount_pct INT, total_usd DOUBLE, "
            "country STRING, payment_method STRING, device STRING, source STRING",
        ),
        "order_time",
    )
    dim_customer = cast_to_timestamp(
        spark.createDataFrame(
            [(100, 1, "1900-01-01 00:00:00", "9999-12-31 23:59:59")],
            "customer_sk BIGINT, customer_id INT, effective_from STRING, effective_to STRING",
        ),
        "effective_from",
        "effective_to",
    )
    dim_country = spark.createDataFrame(
        [("Unknown", 7)], "country_code STRING, country_key BIGINT"
    )
    dim_order_channel = spark.createDataFrame(
        [("card", "mobile", "organic", 3)],
        "payment_method STRING, device STRING, source STRING, order_channel_key BIGINT",
    )

    fact = build_fact_order_items(
        order_items, orders, dim_customer, dim_country, dim_order_channel
    )
    result = {
        (row.order_id, row.line_number): (
            row.date_key,
            row.customer_sk,
            row.country_key,
            row.order_channel_key,
            row.net_amount_usd,
        )
        for row in fact.collect()
    }

    assert result == {
        (10, 1): (20260314, 100, 7, 3, Decimal("9.00")),
        (10, 2): (20260314, 100, 7, 3, Decimal("7.20")),
    }
