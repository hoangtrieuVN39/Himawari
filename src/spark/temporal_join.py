"""Point-in-time temporal join without future data leakage."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

VALID_AIRPORTS = ["VVDN", "VVNB", "VVTS"]


def build_point_in_time_snapshot(
    spark: SparkSession,
    review_slots_utc: list[str],
    metar_df: DataFrame,
    forecast_df: DataFrame,
    himawari_df: DataFrame,
    airports: list[str] | None = None,
    mode: str = "live",
) -> DataFrame:
    """Performs strict point-in-time temporal join using fast HashJoins and pre-filtering."""
    airport_list = airports or VALID_AIRPORTS

    # 1. Fixed Spine: Exactly len(airports) rows per review slot
    slot_rows = [(slot,) for slot in review_slots_utc]
    slots_df = spark.createDataFrame(slot_rows, ["review_slot_str"]).withColumn(
        "review_slot_utc", F.to_timestamp(F.col("review_slot_str"))
    )

    airport_rows = [(icao,) for icao in airport_list]
    airports_df = spark.createDataFrame(airport_rows, ["airport_icao"])
    spine_df = slots_df.crossJoin(airports_df).select("review_slot_utc", "airport_icao")

    max_slot = max(review_slots_utc)

    # 2. METAR Join
    m_src = metar_df.select(
        F.col("airport_icao").alias("m_icao"),
        F.col("observed_at_utc"),
        F.col("fetched_at_utc").alias("metar_fetched_at_utc"),
        F.col("is_backfill").alias("metar_is_backfill"),
        F.col("raw_ob"),
        F.col("visibility_m"),
        F.col("visibility_capped_9999"),
        F.col("is_low_visibility"),
        F.col("has_precip_or_thunder_at_station"),
        F.col("station_weather_codes"),
        F.col("has_vicinity_weather"),
        F.col("vicinity_weather_codes"),
        F.col("trend_weather_codes"),
        F.col("is_amended_or_speci"),
        F.col("revision_index"),
    )
    matched_m = spine_df.join(m_src, on=(F.col("airport_icao") == F.col("m_icao")), how="left")
    cond_m = (F.col("observed_at_utc") <= F.col("review_slot_utc")) & (
        (F.col("metar_fetched_at_utc") <= F.col("review_slot_utc"))
        if mode == "live"
        else ((F.col("metar_fetched_at_utc") <= F.col("review_slot_utc")) | (F.col("metar_is_backfill") == True))
    )
    val_m = matched_m.filter(cond_m | F.col("observed_at_utc").isNull())
    w_m = Window.partitionBy("review_slot_utc", "airport_icao").orderBy(
        F.col("observed_at_utc").desc_nulls_last(),
        F.col("metar_fetched_at_utc").desc_nulls_last(),
        F.col("revision_index").desc_nulls_last(),
    )
    deduped_metar = (
        val_m.withColumn("m_rank", F.row_number().over(w_m))
        .filter(F.col("m_rank") == 1)
        .drop("m_rank", "m_icao", "metar_is_backfill")
        .withColumn("metar_is_present", F.col("observed_at_utc").isNotNull())
        .withColumn(
            "metar_age_minutes",
            F.when(
                F.col("observed_at_utc").isNotNull(),
                F.round(
                    (
                        F.unix_timestamp("review_slot_utc")
                        - F.unix_timestamp("observed_at_utc")
                    )
                    / 60.0,
                    1,
                ),
            ).otherwise(F.lit(None)),
        )
    )

    # 3. Open-Meteo Forecast Join
    f_src = forecast_df.select(
        F.col("airport_icao").alias("f_icao"),
        F.col("forecast_valid_at_utc"),
        F.col("fetched_at_utc").alias("forecast_fetched_at_utc"),
        F.col("is_backfill").alias("forecast_is_backfill"),
        F.col("precipitation_mm"),
        F.col("rain_mm"),
        F.col("showers_mm"),
        F.col("weather_code").alias("forecast_weather_code"),
        F.col("forecast_visibility_m"),
    )
    spine_with_target_hour = spine_df.withColumn(
        "target_forecast_hour_utc",
        F.date_trunc("hour", F.col("review_slot_utc")) + F.expr("INTERVAL 1 HOUR"),
    )
    matched_f = spine_with_target_hour.join(
        f_src,
        on=(
            (F.col("airport_icao") == F.col("f_icao"))
            & (F.col("target_forecast_hour_utc") == F.col("forecast_valid_at_utc"))
        ),
        how="left",
    )
    cond_f = (
        (F.col("forecast_fetched_at_utc") <= F.col("review_slot_utc"))
        if mode == "live"
        else (
            (F.col("forecast_fetched_at_utc") <= F.col("review_slot_utc"))
            | (F.col("forecast_is_backfill") == True)
        )
    )
    val_f = matched_f.filter(cond_f | F.col("forecast_valid_at_utc").isNull())
    w_f = Window.partitionBy("review_slot_utc", "airport_icao").orderBy(
        F.col("forecast_fetched_at_utc").desc_nulls_last()
    )
    deduped_forecast = (
        val_f.withColumn("f_rank", F.row_number().over(w_f))
        .filter(F.col("f_rank") == 1)
        .drop("f_rank", "f_icao", "forecast_is_backfill", "forecast_valid_at_utc")
        .withColumn("forecast_is_present", F.col("forecast_fetched_at_utc").isNotNull())
        .withColumn(
            "forecast_rain_next_hour_mm",
            F.greatest(
                F.coalesce(F.col("precipitation_mm"), F.lit(0.0)),
                F.coalesce(F.col("rain_mm"), F.lit(0.0))
                + F.coalesce(F.col("showers_mm"), F.lit(0.0)),
            ),
        )
    )

    # 4. Himawari B13 Metrics Join
    h_src = himawari_df.select(
        F.col("airport_icao").alias("h_icao"),
        F.col("scan_time_utc").alias("latest_b13_scan_time_utc"),
        F.col("fetched_at_utc").alias("hima_fetched_at_utc"),
        F.col("png_rel_path").alias("latest_b13_png_path"),
        F.col("b13_valid_fraction"),
        F.col("inner_bt_min_k"),
        F.col("inner_bt_min_c"),
        F.col("inner_cold_cloud_fraction"),
        F.col("inner_severe_convection_fraction"),
        F.col("b13_cloud_tag"),
        F.col("b13_summary_vi"),
    )
    matched_h = spine_df.join(h_src, on=(F.col("airport_icao") == F.col("h_icao")), how="left")
    cond_h = F.col("latest_b13_scan_time_utc") <= F.col("review_slot_utc")
    if mode == "live":
        cond_h = cond_h & (F.col("hima_fetched_at_utc") <= F.col("review_slot_utc"))

    val_h = matched_h.filter(cond_h | F.col("latest_b13_scan_time_utc").isNull())
    w_h = Window.partitionBy("review_slot_utc", "airport_icao").orderBy(
        F.col("latest_b13_scan_time_utc").desc_nulls_last(),
        F.col("hima_fetched_at_utc").desc_nulls_last(),
    )
    latest_hima = (
        val_h.withColumn("h_rank", F.row_number().over(w_h))
        .filter(F.col("h_rank") == 1)
        .drop("h_rank", "h_icao", "hima_fetched_at_utc")
        .withColumn("b13_is_present", F.col("latest_b13_scan_time_utc").isNotNull())
    )

    # 5. Join all 3 enriched sources onto spine
    full_snapshot = (
        deduped_metar
        .join(deduped_forecast, on=["review_slot_utc", "airport_icao"], how="left")
        .join(latest_hima, on=["review_slot_utc", "airport_icao"], how="left")
    )

    return full_snapshot

    return full_snapshot
