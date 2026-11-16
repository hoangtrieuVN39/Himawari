"""Spark session + tiny Silver-table builders for the nowcast tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from src.spark.session import get_spark_session, stop_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    try:
        session = get_spark_session("NowcastTests", master="local[2]")
    except Exception as exc:  # pragma: no cover - environment without a JVM
        pytest.skip(f"PySpark JVM gateway unavailable on this environment: {exc}")
    yield session
    stop_spark_session()


def ts(text: str) -> datetime:
    """'2026-11-10 10:30' -> aware UTC datetime (aware values are not shifted by the OS timezone)."""
    return datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)


T0 = "2026-11-10 "


def at(hhmm: str) -> datetime:
    return ts(T0 + hhmm)


_METAR = StructType([
    StructField("airport_icao", StringType()), StructField("observed_at_utc", TimestampType()),
    StructField("fetched_at_utc", TimestampType()), StructField("is_backfill", BooleanType()),
    StructField("revision_index", IntegerType()), StructField("visibility_m", IntegerType()),
    StructField("has_precip_or_thunder_at_station", BooleanType()), StructField("has_vicinity_weather", BooleanType()),
])
_FORECAST = StructType([
    StructField("airport_icao", StringType()), StructField("fetched_at_utc", TimestampType()),
    StructField("forecast_valid_at_utc", TimestampType()), StructField("precipitation_mm", DoubleType()),
    StructField("rain_mm", DoubleType()), StructField("showers_mm", DoubleType()),
    StructField("is_backfill", BooleanType()),
])
_B13 = StructType([
    StructField("airport_icao", StringType()), StructField("scan_time_utc", TimestampType()),
    StructField("fetched_at_utc", TimestampType()), StructField("is_backfill", BooleanType()),
    StructField("status", StringType()), StructField("is_synthetic_test", BooleanType()),
    StructField("inner_bt_min_c", DoubleType()), StructField("inner_cold_cloud_fraction", DoubleType()),
    StructField("inner_severe_convection_fraction", DoubleType()), StructField("b13_valid_fraction", DoubleType()),
])


@pytest.fixture
def frames(spark):
    """Factories for Silver-shaped DataFrames from short tuples."""

    def metar(rows):
        """rows: (icao, observed 'HH:MM', precip, visibility_m[, fetched 'HH:MM' | None, is_backfill])"""
        data = []
        for r in rows:
            icao, obs, precip, vis = r[:4]
            fetched = at(r[4]) if len(r) > 4 and r[4] else at(obs)
            backfill = r[5] if len(r) > 5 else False
            data.append((icao, at(obs), fetched, backfill, 1, vis, precip, False))
        return spark.createDataFrame(data, _METAR)

    def forecast(rows):
        """rows: (icao, fetched 'HH:MM', valid 'HH:MM', precipitation, rain, showers[, is_backfill])"""
        data = [(r[0], at(r[1]), at(r[2]), r[3], r[4], r[5], r[6] if len(r) > 6 else False) for r in rows]
        return spark.createDataFrame(data, _FORECAST)

    def b13(rows):
        """rows: (icao, scan 'HH:MM', bt_min_c[, fetched 'HH:MM' | None, is_backfill])"""
        data = []
        for r in rows:
            fetched = at(r[3]) if len(r) > 3 and r[3] else at(r[1])
            data.append((r[0], at(r[1]), fetched, r[4] if len(r) > 4 else False, "AVAILABLE", False, r[2], 0.1, 0.0, 1.0))
        return spark.createDataFrame(data, _B13)

    def samples(rows):
        """rows: (icao, 'HH:MM')"""
        return spark.createDataFrame(
            [(i, at(h)) for i, h in rows],
            StructType([StructField("airport_icao", StringType()), StructField("sample_time_utc", TimestampType())]),
        )

    return type("Frames", (), {"metar": staticmethod(metar), "forecast": staticmethod(forecast),
                               "b13": staticmethod(b13), "samples": staticmethod(samples)})

