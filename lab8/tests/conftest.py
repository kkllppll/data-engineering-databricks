import os

import pytest


@pytest.fixture(scope="session")
def spark():
    # CI runs the tests on a local Spark (no Databricks access needed);
    # locally they run on Databricks serverless through Databricks Connect.
    if os.getenv("SPARK_LOCAL") == "1":
        from pyspark.sql import SparkSession

        return (
            SparkSession.builder.master("local[2]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.getOrCreate()
