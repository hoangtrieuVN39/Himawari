"""Shared PySpark test fixtures."""

import pytest
from pyspark.sql import SparkSession
from src.spark.session import get_spark_session, stop_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession for fast test execution."""
    try:
        spark_session = get_spark_session("PySparkAirportTests", master="local[2]")
    except Exception as exc:
        pytest.skip(f"PySpark JVM gateway unavailable on this environment: {exc}")
    yield spark_session
    stop_spark_session()

