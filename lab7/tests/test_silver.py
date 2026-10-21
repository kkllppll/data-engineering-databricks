from datetime import datetime
from pyspark.testing import assertDataFrameEqual
from orders_pipeline.silver import (
    add_failed_rules,
    add_line_number,
    flag_known,
    keep_latest,
    validate_order_items,
    validate_orders,
)

DAY_1 = datetime(2026, 1, 1)
DAY_2 = datetime(2026, 1, 2)


def test_keep_latest_keeps_most_recent_version(spark):
    df = spark.createDataFrame(
        [(1, "old", DAY_1), (1, "new", DAY_2), (2, "only", DAY_1)],
        "order_id INT, status STRING, ingestion_timestamp TIMESTAMP",
    )
    expected = spark.createDataFrame([(1, "new", DAY_2), (2, "only", DAY_1)], df.schema)

    assertDataFrameEqual(keep_latest(df, "order_id", "ingestion_timestamp"), expected)


def test_keep_latest_keeps_null_keys(spark):
    df = spark.createDataFrame(
        [(None, "a", DAY_1), (None, "b", DAY_2), (1, "c", DAY_1)],
        "order_id INT, status STRING, ingestion_timestamp TIMESTAMP",
    )

    assert keep_latest(df, "order_id", "ingestion_timestamp").count() == 3


def test_flag_known_ignores_duplicate_reference(spark):
    orders = spark.createDataFrame([(1, 10), (2, 99)], "order_id INT, customer_id INT")
    customers = spark.createDataFrame([(10,), (10,)], "customer_id INT")
    expected = spark.createDataFrame(
        [(1, True), (2, None)], "order_id INT, _customer_known BOOLEAN"
    )

    result = flag_known(orders, customers, "customer_id", "_customer_known")

    assertDataFrameEqual(result.select("order_id", "_customer_known"), expected)


def test_add_failed_rules_lists_broken_rules(spark):
    df = spark.createDataFrame(
        [(1, 50), (2, -5), (3, 500), (4, None)], "id INT, amount INT"
    )
    rules = {"positive": "amount > 0", "below_limit": "amount < 100"}

    result = {row.id: row.failed_rules for row in add_failed_rules(df, rules).collect()}

    assert result == {
        1: [],
        2: ["positive"],
        3: ["below_limit"],
        4: ["positive", "below_limit"],
    }


def test_validate_orders_rejects_invalid_orders(spark):
    orders = spark.createDataFrame(
        [
            (1, 10, DAY_1, 10, 100.0, 90.0, DAY_1),
            (2, 10, DAY_1, 10, 100.0, 95.0, DAY_1),
            (3, 77, DAY_1, 0, 50.0, 50.0, DAY_1),
        ],
        "order_id INT, customer_id INT, order_time TIMESTAMP, discount_pct INT, "
        "subtotal_usd DOUBLE, total_usd DOUBLE, ingestion_timestamp TIMESTAMP",
    )
    customers = spark.createDataFrame([(10,)], "customer_id INT")

    result = {
        row.order_id: row.failed_rules
        for row in validate_orders(orders, customers).collect()
    }

    assert result == {1: [], 2: ["total_matches_discount"], 3: ["customer_exists"]}


def test_add_line_number_per_order(spark):
    items = spark.createDataFrame(
        [
            (1, 5, 2.0, 1, 2.0),
            (1, 3, 1.0, 2, 2.0),
            (1, 5, 2.0, 3, 6.0),
            (2, 9, 4.0, 1, 4.0),
        ],
        "order_id INT, product_id INT, unit_price_usd DOUBLE, quantity INT, line_total_usd DOUBLE",
    )

    result = {
        (row.order_id, row.product_id, row.quantity): row.line_number
        for row in add_line_number(items).collect()
    }

    assert result == {(1, 3, 2): 1, (1, 5, 1): 2, (1, 5, 3): 3, (2, 9, 1): 1}


def test_validate_order_items_rejects_unknown_references_and_rescued_data(spark):
    items = spark.createDataFrame(
        [
            (1, 5, 2.0, 1, 2.0, None),
            (1, 99, 2.0, 1, 2.0, None),
            (2, 5, 2.0, 1, 2.0, '{"extra_column": "x"}'),
        ],
        "order_id INT, product_id INT, unit_price_usd DOUBLE, quantity INT, "
        "line_total_usd DOUBLE, _rescued_data STRING",
    )
    orders = spark.createDataFrame([(1,)], "order_id INT")
    products = spark.createDataFrame([(5,)], "product_id INT")

    result = {
        (row.order_id, row.product_id): row.failed_rules
        for row in validate_order_items(items, orders, products).collect()
    }

    assert result == {
        (1, 5): [],
        (1, 99): ["product_exists"],
        (2, 5): ["no_rescued_data", "order_exists"],
    }
