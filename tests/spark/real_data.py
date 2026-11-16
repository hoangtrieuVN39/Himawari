"""Reads the real 2026-11-09 Bronze data through the active (daily_jsonl) raw layout."""

from pyspark.sql import DataFrame, SparkSession
from src.ingestion.raw_store import RawEnvelopeStore
from src.spark.readers import (
    read_raw_himawari_metrics,
    read_raw_metar,
    read_raw_open_meteo,
)

REAL_DATE = "2026-11-09"


def _spec(source: str):
    return RawEnvelopeStore(storage_root="data").read_specs(source, REAL_DATE)[0]


def real_raw_metar(spark: SparkSession) -> DataFrame:
    spec = _spec("metar")
    return read_raw_metar(spark, list(spec.paths), multiline=spec.multiline)


def real_raw_open_meteo(spark: SparkSession) -> DataFrame:
    spec = _spec("open_meteo")
    return read_raw_open_meteo(spark, list(spec.paths), multiline=spec.multiline)


def real_raw_himawari_metrics(spark: SparkSession) -> DataFrame:
    spec = _spec("himawari_metrics")
    return read_raw_himawari_metrics(spark, list(spec.paths), multiline=spec.multiline)
