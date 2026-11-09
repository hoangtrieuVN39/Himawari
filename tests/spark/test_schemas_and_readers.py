"""Unit tests for strict schemas and raw envelope readers."""

from pyspark.sql import SparkSession
from src.spark.readers import (
    read_raw_himawari_metrics,
    read_raw_metar,
    read_raw_open_meteo,
)


def test_read_real_metar_raw_envelopes(spark: SparkSession):
    df = read_raw_metar(spark, "data/raw/metar/date=2026-11-09/*.json")
    assert df.count() > 0
    assert "airport_icao" in df.columns
    assert "observed_at_utc" in df.columns
    assert "raw_ob" in df.columns
    assert "input_file_path" in df.columns

    sample_airports = {
        row.airport_icao for row in df.select("airport_icao").distinct().collect()
    }
    assert sample_airports.issubset({"VVDN", "VVNB", "VVTS"})


def test_read_real_open_meteo_raw_envelopes(spark: SparkSession):
    df = read_raw_open_meteo(spark, "data/raw/open_meteo/date=2026-11-09/*.json")
    assert df.count() > 0
    assert "forecast_valid_at_utc" in df.columns
    assert "precipitation_mm" in df.columns
    assert "airport_icao" in df.columns

    sample_airports = {
        row.airport_icao for row in df.select("airport_icao").distinct().collect()
    }
    assert sample_airports.issubset({"VVDN", "VVNB", "VVTS"})


def test_read_real_himawari_metrics_envelopes(spark: SparkSession):
    df = read_raw_himawari_metrics(spark, "data/raw/himawari_metrics/date=2026-11-09/*.json")
    assert df.count() > 0
    assert "scan_time_utc" in df.columns
    assert "b13_valid_fraction" in df.columns
    assert "inner_bt_min_k" in df.columns
    assert "airport_icao" in df.columns

    sample_airports = {
        row.airport_icao for row in df.select("airport_icao").distinct().collect()
    }
    assert sample_airports.issubset({"VVDN", "VVNB", "VVTS"})


def test_corrupt_json_envelope_captured(spark: SparkSession, tmp_path):
    bad_file = tmp_path / "bad_envelope.json"
    bad_file.write_text("{ this is not valid json")

    df = read_raw_metar(spark, str(bad_file))
    corrupt_rows = df.filter(df._corrupt_record.isNotNull()).count()
    assert corrupt_rows >= 1
