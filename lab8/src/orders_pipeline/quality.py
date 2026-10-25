"""Data-quality result summaries and reconciliation between medallion layers"""

from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MONEY = "decimal(12,2)"


def summarize_dqx_results(
    checked: DataFrame, table_name: str, checks: list[dict]
) -> list[dict]:
    """Returns one result per check with the number of rows the check flagged in a DQX output."""
    flagged = checked.select(F.explode("_errors.name").alias("check_name")).unionByName(
        checked.select(F.explode("_warnings.name").alias("check_name"))
    )
    failed_rows = {
        row.check_name: row["count"]
        for row in flagged.groupBy("check_name").count().collect()
    }
    total_rows = checked.count()
    return [
        {
            "check_type": "dqx",
            "table_name": table_name,
            "check_name": check["name"],
            "dimension": check["user_metadata"]["dimension"],
            "criticality": check["criticality"],
            "failed_rows": failed_rows.get(check["name"], 0),
            "total_rows": total_rows,
            "expected_value": None,
            "actual_value": None,
            "passed": failed_rows.get(check["name"], 0) == 0,
        }
        for check in checks
    ]


def money_sum(df: DataFrame, column: str) -> Decimal:
    # each value is rounded to cents before summing, exactly as amounts are stored in gold
    return df.agg(F.sum(F.col(column).cast(MONEY))).first()[0] or Decimal("0")


def reconcile(
    table_name: str, check_name: str, expected: int | Decimal, actual: int | Decimal
) -> dict:
    expected, actual = Decimal(expected), Decimal(actual)
    return {
        "check_type": "reconciliation",
        "table_name": table_name,
        "check_name": check_name,
        "dimension": "reconciliation",
        "criticality": "error",
        "failed_rows": None,
        "total_rows": None,
        "expected_value": expected,
        "actual_value": actual,
        "passed": expected == actual,
    }


def blocking_failures(results: list[dict]) -> list[dict]:
    return [
        result
        for result in results
        if result["criticality"] == "error" and not result["passed"]
    ]
