"""Shared PySpark test fixtures."""

import pytest
from pyspark.sql import SparkSession
from src.spark.session import get_spark_session, stop_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession for fast test execution."""
    spark_session = get_spark_session("PySparkAirportTests", master="local[2]")
    yield spark_session
    stop_spark_session()

