from decimal import Decimal

from orders_pipeline.quality import (
    blocking_failures,
    money_sum,
    reconcile,
    summarize_dqx_results,
)


def test_summarize_dqx_results_counts_flagged_rows_per_check(spark):
    checked = spark.createDataFrame(
        [
            (1, [("not_null",)], None),
            (2, [("not_null",)], [("in_list",)]),
            (3, None, None),
        ],
        "id INT, _errors ARRAY<STRUCT<name: STRING>>, _warnings ARRAY<STRUCT<name: STRING>>",
    )
    checks = [
        {
            "name": "not_null",
            "criticality": "error",
            "user_metadata": {"dimension": "completeness"},
        },
        {
            "name": "in_list",
            "criticality": "warn",
            "user_metadata": {"dimension": "validity"},
        },
        {
            "name": "unique",
            "criticality": "error",
            "user_metadata": {"dimension": "uniqueness"},
        },
    ]

    result = {
        row["check_name"]: (row["failed_rows"], row["total_rows"], row["passed"])
        for row in summarize_dqx_results(checked, "t", checks)
    }

    assert result == {
        "not_null": (2, 3, False),
        "in_list": (1, 3, False),
        "unique": (0, 3, True),
    }


def test_money_sum_rounds_each_value_to_cents(spark):
    df = spark.createDataFrame([(0.125,), (0.125,)], "amount DOUBLE")

    assert money_sum(df, "amount") == Decimal("0.26")
    assert money_sum(df.limit(0), "amount") == Decimal("0")


def test_reconcile_passes_only_on_exact_match():
    assert reconcile("t", "c", 10, 10)["passed"]
    assert not reconcile("t", "c", Decimal("10.00"), Decimal("10.01"))["passed"]


def test_blocking_failures_ignores_warnings_and_passed_checks():
    results = [
        {"check_name": "a", "criticality": "error", "passed": False},
        {"check_name": "b", "criticality": "warn", "passed": False},
        {"check_name": "c", "criticality": "error", "passed": True},
    ]

    assert [result["check_name"] for result in blocking_failures(results)] == ["a"]
