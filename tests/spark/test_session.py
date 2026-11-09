"""Test SparkSession configuration and basic operations."""

from pyspark.sql import SparkSession
from src.spark.session import get_spark_session


def test_spark_session_utc_and_basic_dataframe(spark: SparkSession):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    df = spark.createDataFrame([("VVDN", 1), ("VVNB", 2), ("VVTS", 3)], ["airport_icao", "rank"])
    assert df.count() == 3
    airports = [row.airport_icao for row in df.collect()]
    assert airports == ["VVDN", "VVNB", "VVTS"]
