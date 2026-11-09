"""PySpark SQL StructType schemas for raw JSON envelopes and processed datasets."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

METAR_PAYLOAD_ITEM_SCHEMA = StructType(
    [
        StructField("icaoId", StringType(), True),
        StructField("receiptTime", StringType(), True),
        StructField("obsTime", LongType(), True),
        StructField("reportTime", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("dewp", DoubleType(), True),
        StructField("wdir", StringType(), True),
        StructField("wspd", IntegerType(), True),
        StructField("wgst", IntegerType(), True),
        StructField("visib", StringType(), True),
        StructField("altim", IntegerType(), True),
        StructField("qcField", IntegerType(), True),
        StructField("metarType", StringType(), True),
        StructField("rawOb", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("elev", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("cover", StringType(), True),
        StructField("fltCat", StringType(), True),
        StructField("wxString", StringType(), True),
    ]
)

METAR_ENVELOPE_SCHEMA = StructType(
    [
        StructField(
            "metadata",
            StructType(
                [
                    StructField("batch_id", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("source_url", StringType(), True),
                    StructField("attribution", StringType(), True),
                    StructField("expected_slot_utc", StringType(), True),
                    StructField("fetched_at_utc", StringType(), True),
                    StructField("http_status", IntegerType(), True),
                    StructField("airports_requested", ArrayType(StringType()), True),
                    StructField("is_backfill", BooleanType(), True),
                    StructField("record_count", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField("payload", ArrayType(METAR_PAYLOAD_ITEM_SCHEMA), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

OPEN_METEO_ENVELOPE_SCHEMA = StructType(
    [
        StructField(
            "metadata",
            StructType(
                [
                    StructField("batch_id", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("airport_icao", StringType(), True),
                    StructField("airport_name", StringType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                    StructField("source_url", StringType(), True),
                    StructField("attribution", StringType(), True),
                    StructField("expected_slot_utc", StringType(), True),
                    StructField("fetched_at_utc", StringType(), True),
                    StructField("http_status", IntegerType(), True),
                    StructField("forecast_hours_count", IntegerType(), True),
                    StructField("is_backfill", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "payload",
            StructType(
                [
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                    StructField("generationtime_ms", DoubleType(), True),
                    StructField("utc_offset_seconds", IntegerType(), True),
                    StructField("timezone", StringType(), True),
                    StructField("timezone_abbreviation", StringType(), True),
                    StructField("elevation", DoubleType(), True),
                    StructField(
                        "hourly",
                        StructType(
                            [
                                StructField("time", ArrayType(StringType()), True),
                                StructField("precipitation", ArrayType(DoubleType()), True),
                                StructField("rain", ArrayType(DoubleType()), True),
                                StructField("showers", ArrayType(DoubleType()), True),
                                StructField("weather_code", ArrayType(IntegerType()), True),
                                StructField("visibility", ArrayType(DoubleType()), True),
                                StructField("wind_speed_10m", ArrayType(DoubleType()), True),
                                StructField("wind_gusts_10m", ArrayType(DoubleType()), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("_corrupt_record", StringType(), True),
    ]
)

HIMAWARI_AIRPORT_ITEM_SCHEMA = StructType(
    [
        StructField("airport_icao", StringType(), True),
        StructField("segment_code", StringType(), True),
        StructField("scan_time_utc", StringType(), True),
        StructField("fetched_at_utc", StringType(), True),
        StructField("status", StringType(), True),
        StructField("raw_file", StringType(), True),
        StructField("png_rel_path", StringType(), True),
        StructField("png_path", StringType(), True),
        StructField("outer_total_pixels", LongType(), True),
        StructField("outer_valid_pixels", LongType(), True),
        StructField("b13_valid_fraction", DoubleType(), True),
        StructField("outer_bt_min_k", DoubleType(), True),
        StructField("outer_bt_min_c", DoubleType(), True),
        StructField("outer_bt_mean_k", DoubleType(), True),
        StructField("outer_bt_mean_c", DoubleType(), True),
        StructField("outer_cold_cloud_fraction", DoubleType(), True),
        StructField("outer_severe_convection_fraction", DoubleType(), True),
        StructField("inner_total_pixels", LongType(), True),
        StructField("inner_valid_pixels", LongType(), True),
        StructField("inner_valid_fraction", DoubleType(), True),
        StructField("inner_bt_min_k", DoubleType(), True),
        StructField("inner_bt_min_c", DoubleType(), True),
        StructField("inner_bt_mean_k", DoubleType(), True),
        StructField("inner_bt_mean_c", DoubleType(), True),
        StructField("inner_cold_cloud_fraction", DoubleType(), True),
        StructField("inner_severe_convection_fraction", DoubleType(), True),
    ]
)

HIMAWARI_METRICS_ENVELOPE_SCHEMA = StructType(
    [
        StructField(
            "metadata",
            StructType(
                [
                    StructField("batch_id", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("scan_slot_utc", StringType(), True),
                    StructField("processed_at_utc", StringType(), True),
                    StructField("is_backfill", BooleanType(), True),
                    StructField("is_synthetic_test", BooleanType(), True),
                    StructField("attribution", StringType(), True),
                    StructField("airport_count", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField("airports", ArrayType(HIMAWARI_AIRPORT_ITEM_SCHEMA), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)
