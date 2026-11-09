"""Raw JSON envelope readers for METAR, Open-Meteo, and Himawari-9 B13."""

import glob
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.spark.schemas import (
    HIMAWARI_METRICS_ENVELOPE_SCHEMA,
    METAR_ENVELOPE_SCHEMA,
    OPEN_METEO_ENVELOPE_SCHEMA,
)


def _resolve_paths(path: str | list[str]) -> list[str]:
    """Resolves local wildcard patterns using Python glob to bypass Hadoop Windows glob bugs."""
    if isinstance(path, list):
        resolved = []
        for p in path:
            resolved.extend(_resolve_paths(p))
        return resolved

    if path.startswith("hdfs://"):
        return [path]

    matched = glob.glob(path)
    if not matched:
        # Check if it's an exact file or dir
        if Path(path).exists():
            return [path.replace("\\", "/")]
        return [path]
    return [p.replace("\\", "/") for p in sorted(matched)]


def read_raw_metar(spark: SparkSession, path: str | list[str]) -> DataFrame:
    """Reads raw METAR JSON envelopes and explodes payload into airport observation rows."""
    files = _resolve_paths(path)
    raw_df = (
        spark.read.schema(METAR_ENVELOPE_SCHEMA)
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(files)
        .withColumn("input_file_path", F.input_file_name())
    )

    exploded_df = raw_df.select(
        F.col("metadata.batch_id").alias("batch_id"),
        F.col("metadata.source").alias("source"),
        F.to_timestamp(F.col("metadata.fetched_at_utc")).alias("fetched_at_utc"),
        F.coalesce(F.col("metadata.is_backfill"), F.lit(False)).alias("is_backfill"),
        F.col("_corrupt_record"),
        F.col("input_file_path"),
        F.posexplode_outer("payload").alias("item_index", "item"),
    )

    return exploded_df.select(
        F.col("batch_id"),
        F.col("source"),
        F.col("fetched_at_utc"),
        F.col("is_backfill"),
        F.col("item.icaoId").alias("airport_icao"),
        F.coalesce(
            F.to_timestamp(F.from_unixtime(F.col("item.obsTime"))),
            F.to_timestamp(F.col("item.reportTime")),
        ).alias("observed_at_utc"),
        F.col("item.obsTime").alias("obs_time_epoch"),
        F.col("item.reportTime").alias("report_time_raw"),
        F.to_timestamp(F.col("item.receiptTime")).alias("receipt_time_utc"),
        F.col("item.rawOb").alias("raw_ob"),
        F.col("item.visib").alias("visib_raw"),
        F.col("item.wxString").alias("wx_string_raw"),
        F.col("item.temp").alias("temp_c"),
        F.col("item.dewp").alias("dewp_c"),
        F.col("item.wdir").alias("wdir_deg"),
        F.col("item.wspd").alias("wspd_kt"),
        F.col("item.wgst").alias("wgst_kt"),
        F.col("item.altim").alias("altim_hpa"),
        F.col("item.fltCat").alias("flt_cat"),
        F.col("_corrupt_record"),
        F.col("input_file_path"),
    )


def read_raw_open_meteo(spark: SparkSession, path: str | list[str]) -> DataFrame:
    """Reads raw Open-Meteo JSON envelopes and explodes hourly forecast arrays."""
    files = _resolve_paths(path)
    raw_df = (
        spark.read.schema(OPEN_METEO_ENVELOPE_SCHEMA)
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(files)
        .withColumn("input_file_path", F.input_file_name())
    )

    hourly_zipped = F.arrays_zip(
        F.col("payload.hourly.time"),
        F.col("payload.hourly.precipitation"),
        F.col("payload.hourly.rain"),
        F.col("payload.hourly.showers"),
        F.col("payload.hourly.weather_code"),
        F.col("payload.hourly.visibility"),
        F.col("payload.hourly.wind_speed_10m"),
        F.col("payload.hourly.wind_gusts_10m"),
    )

    exploded_df = raw_df.select(
        F.col("metadata.batch_id").alias("batch_id"),
        F.col("metadata.source").alias("source"),
        F.col("metadata.airport_icao").alias("airport_icao"),
        F.to_timestamp(F.col("metadata.fetched_at_utc")).alias("fetched_at_utc"),
        F.coalesce(F.col("metadata.is_backfill"), F.lit(False)).alias("is_backfill"),
        F.col("_corrupt_record"),
        F.col("input_file_path"),
        F.explode_outer(hourly_zipped).alias("h"),
    )

    return exploded_df.select(
        F.col("batch_id"),
        F.col("source"),
        F.col("airport_icao"),
        F.col("fetched_at_utc"),
        F.col("is_backfill"),
        F.to_timestamp(F.col("h.time")).alias("forecast_valid_at_utc"),
        F.col("h.precipitation").alias("precipitation_mm"),
        F.col("h.rain").alias("rain_mm"),
        F.col("h.showers").alias("showers_mm"),
        F.col("h.weather_code").alias("weather_code"),
        F.col("h.visibility").alias("forecast_visibility_m"),
        F.col("h.wind_speed_10m").alias("wind_speed_10m_kmh"),
        F.col("h.wind_gusts_10m").alias("wind_gusts_10m_kmh"),
        F.col("_corrupt_record"),
        F.col("input_file_path"),
    )


def read_raw_himawari_metrics(spark: SparkSession, path: str | list[str]) -> DataFrame:
    """Reads raw Himawari metrics JSON envelopes and explodes airport items."""
    files = _resolve_paths(path)
    raw_df = (
        spark.read.schema(HIMAWARI_METRICS_ENVELOPE_SCHEMA)
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(files)
        .withColumn("input_file_path", F.input_file_name())
    )

    exploded_df = raw_df.select(
        F.col("metadata.batch_id").alias("batch_id"),
        F.col("metadata.source").alias("source"),
        F.to_timestamp(F.col("metadata.scan_slot_utc")).alias("scan_slot_utc"),
        F.to_timestamp(F.col("metadata.processed_at_utc")).alias("processed_at_utc"),
        F.coalesce(F.col("metadata.is_backfill"), F.lit(False)).alias("is_backfill"),
        F.coalesce(F.col("metadata.is_synthetic_test"), F.lit(False)).alias("is_synthetic_test"),
        F.col("_corrupt_record"),
        F.col("input_file_path"),
        F.explode_outer("airports").alias("a"),
    )

    return exploded_df.select(
        F.col("batch_id"),
        F.col("source"),
        F.col("scan_slot_utc"),
        F.col("processed_at_utc"),
        F.col("is_backfill"),
        F.col("is_synthetic_test"),
        F.col("a.airport_icao").alias("airport_icao"),
        F.col("a.segment_code").alias("segment_code"),
        F.to_timestamp(F.col("a.scan_time_utc")).alias("scan_time_utc"),
        F.to_timestamp(F.col("a.fetched_at_utc")).alias("fetched_at_utc"),
        F.col("a.status").alias("status"),
        F.col("a.png_rel_path").alias("png_rel_path"),
        F.col("a.outer_total_pixels").alias("outer_total_pixels"),
        F.col("a.outer_valid_pixels").alias("outer_valid_pixels"),
        F.col("a.b13_valid_fraction").alias("b13_valid_fraction"),
        F.col("a.outer_bt_min_k").alias("outer_bt_min_k"),
        F.col("a.outer_bt_min_c").alias("outer_bt_min_c"),
        F.col("a.outer_bt_mean_k").alias("outer_bt_mean_k"),
        F.col("a.outer_bt_mean_c").alias("outer_bt_mean_c"),
        F.col("a.outer_cold_cloud_fraction").alias("outer_cold_cloud_fraction"),
        F.col("a.outer_severe_convection_fraction").alias("outer_severe_convection_fraction"),
        F.col("a.inner_total_pixels").alias("inner_total_pixels"),
        F.col("a.inner_valid_pixels").alias("inner_valid_pixels"),
        F.col("a.inner_valid_fraction").alias("inner_valid_fraction"),
        F.col("a.inner_bt_min_k").alias("inner_bt_min_k"),
        F.col("a.inner_bt_min_c").alias("inner_bt_min_c"),
        F.col("a.inner_bt_mean_k").alias("inner_bt_mean_k"),
        F.col("a.inner_bt_mean_c").alias("inner_bt_mean_c"),
        F.col("a.inner_cold_cloud_fraction").alias("inner_cold_cloud_fraction"),
        F.col("a.inner_severe_convection_fraction").alias("inner_severe_convection_fraction"),
        F.col("_corrupt_record"),
        F.col("input_file_path"),
    )
