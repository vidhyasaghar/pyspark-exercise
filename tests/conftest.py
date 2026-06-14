"""Shared pytest fixtures."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("pyspark-exercise-tests").config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()  # type: ignore[attr-defined]
