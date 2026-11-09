"""Priority ranking engine (Level 1–4) and co-priority tie resolution."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def apply_priority_rules(snapshot_df: DataFrame) -> DataFrame:
    """Evaluates Level 1–4 priority rules, builds Vietnamese explanations, and flags co-priority top ties."""
    is_level_1 = (
        (F.col("metar_is_present") == True)
        & (F.col("metar_age_minutes") <= 60.0)
        & (
            (F.col("has_precip_or_thunder_at_station") == True)
            | (F.col("is_low_visibility") == True)
        )
    )

    is_level_2 = (~is_level_1) & (
        (F.col("metar_is_present") == False) | (F.col("metar_age_minutes") > 60.0)
    )

    is_level_3 = (
        (~is_level_1)
        & (~is_level_2)
        & (F.col("forecast_is_present") == True)
        & (F.col("forecast_rain_next_hour_mm") >= 0.1)
    )

    priority_level_col = (
        F.when(is_level_1, F.lit(1))
        .when(is_level_2, F.lit(2))
        .when(is_level_3, F.lit(3))
        .otherwise(F.lit(4))
    )

    priority_label_col = (
        F.when(is_level_1, F.lit("LEVEL_1_HAZARD_AT_STATION"))
        .when(is_level_2, F.lit("LEVEL_2_DATA_VERIFICATION"))
        .when(is_level_3, F.lit("LEVEL_3_FORECAST_RAIN_ALERT"))
        .otherwise(F.lit("LEVEL_4_ROUTINE"))
    )

    # Human-readable Vietnamese reason string
    wx_codes_str = F.concat_ws(", ", F.col("station_weather_codes"))
    reason_level_1 = F.when(
        (F.col("has_precip_or_thunder_at_station") == True)
        & (F.col("is_low_visibility") == True),
        F.concat(
            F.lit("METAR mới ("),
            F.date_format("observed_at_utc", "HH:mm'Z'"),
            F.lit(") ghi nhận mưa/dông tại trạm ("),
            wx_codes_str,
            F.lit(") và tầm nhìn thấp "),
            F.col("visibility_m").cast("string"),
            F.lit("m (< 5km)"),
        ),
    ).when(
        F.col("has_precip_or_thunder_at_station") == True,
        F.concat(
            F.lit("METAR mới ("),
            F.date_format("observed_at_utc", "HH:mm'Z'"),
            F.lit(") ghi nhận mưa/dông tại trạm ("),
            wx_codes_str,
            F.lit("), tầm nhìn "),
            F.coalesce(F.col("visibility_m").cast("string"), F.lit("không rõ")),
            F.lit("m"),
        ),
    ).otherwise(
        F.concat(
            F.lit("METAR mới ghi nhận tầm nhìn thấp "),
            F.col("visibility_m").cast("string"),
            F.lit("m (< 5km)"),
        )
    )

    reason_level_2 = F.when(
        F.col("metar_is_present") == False,
        F.lit("Cần xác minh dữ liệu: Thiếu bản tin quan trắc METAR"),
    ).otherwise(
        F.concat(
            F.lit("Cần xác minh dữ liệu: Bản tin METAR đã cũ "),
            F.col("metar_age_minutes").cast("string"),
            F.lit(" phút (> 60 phút)"),
        )
    )

    reason_level_3 = F.concat(
        F.lit("Điều kiện tại trạm tốt, nhưng dự báo mưa giờ tới đạt "),
        F.round(F.col("forecast_rain_next_hour_mm"), 1).cast("string"),
        F.lit(" mm (hiệu lực "),
        F.date_format("target_forecast_hour_utc", "HH:mm'Z'"),
        F.lit(")"),
    )

    reason_level_4 = F.lit("Điều kiện tại trạm bình thường, không có cảnh báo mưa giờ tới")

    reason_vi_col = (
        F.when(is_level_1, reason_level_1)
        .when(is_level_2, reason_level_2)
        .when(is_level_3, reason_level_3)
        .otherwise(reason_level_4)
    )

    ranked_df = (
        snapshot_df.withColumn("priority_level", priority_level_col)
        .withColumn("priority_label", priority_label_col)
        .withColumn("reason_vi", reason_vi_col)
    )

    # Tie resolution window
    slot_window = Window.partitionBy("review_slot_utc")
    with_top = (
        ranked_df.withColumn("top_priority_level", F.min("priority_level").over(slot_window))
        .withColumn("is_top_priority", F.col("priority_level") == F.col("top_priority_level"))
        .withColumn(
            "accepted_first_review_airports",
            F.sort_array(
                F.collect_set(
                    F.when(F.col("is_top_priority"), F.col("airport_icao"))
                ).over(slot_window)
            ),
        )
        .withColumn("co_priority_top_count", F.size("accepted_first_review_airports"))
        .withColumn(
            "is_co_priority_top",
            F.col("is_top_priority") & (F.col("co_priority_top_count") > 1),
        )
    )

    return with_top
