"""Meteorological transformations: METAR visibility, phenomenon parser, and B13 cloud classification."""

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PRECIP_TS_PATTERN = re.compile(r"^(\+|-)?(TS|SH|FZ|BL|DR)?(RA|DZ|SN|SG|IC|PL|GR|GS|UP|SQ|FC|TS)+$")
VICINITY_PATTERN = re.compile(r"^VC(TS|SH|FG|RA|PO|BL|DS|SS)+$")
METRIC_VISIBILITY_PATTERN = re.compile(r"^\d{4}$")
TREND_SPLIT_PATTERN = re.compile(r"\b(TEMPO|BECMG|NOSIG|RMK)\b")


def parse_metar_raw_ob(raw_ob: str | None, visib_raw: str | None = None) -> dict:
    """Deterministic regex parser extracting visibility in meters and separating station vs vicinity weather."""
    if not raw_ob:
        return {
            "visibility_m": None,
            "visibility_capped_9999": False,
            "is_low_visibility": False,
            "has_precip_or_thunder_at_station": False,
            "station_weather_codes": [],
            "has_vicinity_weather": False,
            "vicinity_weather_codes": [],
            "trend_weather_codes": [],
            "is_amended_or_speci": False,
        }

    is_amended_or_speci = (
        raw_ob.startswith("SPECI")
        or " COR " in raw_ob
        or " RTD " in raw_ob
        or " AMD " in raw_ob
    )

    match = TREND_SPLIT_PATTERN.search(raw_ob)
    if match:
        main_body = raw_ob[: match.start()]
        trend_body = raw_ob[match.start() :]
    else:
        main_body = raw_ob
        trend_body = ""

    main_tokens = main_body.split()
    trend_tokens = trend_body.split()

    visibility_m = None
    visibility_capped_9999 = False

    if "CAVOK" in main_tokens:
        visibility_m = 10000
        visibility_capped_9999 = True
    else:
        for token in main_tokens:
            if token == "9999":
                visibility_m = 10000
                visibility_capped_9999 = True
                break
            if (
                METRIC_VISIBILITY_PATTERN.match(token)
                and not token.endswith("Z")
                and not token.startswith("Q")
                and not token.startswith("A")
            ):
                val = int(token)
                if val >= 9999:
                    visibility_m = 10000
                    visibility_capped_9999 = True
                else:
                    visibility_m = val
                    visibility_capped_9999 = False
                break

    if visibility_m is None and visib_raw:
        visib_clean = str(visib_raw).strip()
        if visib_clean in ("6+", "10+"):
            visibility_m = 10000
            visibility_capped_9999 = True
        else:
            try:
                miles = float(visib_clean)
                visibility_m = int(round(miles * 1609.34))
                visibility_capped_9999 = miles >= 6.0
            except ValueError:
                pass

    is_low_visibility = visibility_m is not None and visibility_m < 5000

    station_weather = []
    vicinity_weather = []
    for token in main_tokens:
        if VICINITY_PATTERN.match(token):
            vicinity_weather.append(token)
        elif PRECIP_TS_PATTERN.match(token):
            station_weather.append(token)

    has_precip_or_thunder_at_station = len(station_weather) > 0
    has_vicinity_weather = len(vicinity_weather) > 0

    trend_weather = []
    for token in trend_tokens:
        if PRECIP_TS_PATTERN.match(token) or VICINITY_PATTERN.match(token):
            trend_weather.append(token)

    return {
        "visibility_m": visibility_m,
        "visibility_capped_9999": visibility_capped_9999,
        "is_low_visibility": is_low_visibility,
        "has_precip_or_thunder_at_station": has_precip_or_thunder_at_station,
        "station_weather_codes": station_weather,
        "has_vicinity_weather": has_vicinity_weather,
        "vicinity_weather_codes": vicinity_weather,
        "trend_weather_codes": trend_weather,
        "is_amended_or_speci": is_amended_or_speci,
    }


def enrich_valid_metar(df: DataFrame) -> DataFrame:
    """Enriches valid METAR records with standardized visibility and weather categories using native Spark SQL."""
    is_amended_or_speci = (
        F.col("raw_ob").startswith("SPECI")
        | F.col("raw_ob").contains(" COR ")
        | F.col("raw_ob").contains(" RTD ")
        | F.col("raw_ob").contains(" AMD ")
    )

    # Split main body vs trend section at TEMPO/BECMG/NOSIG/RMK
    main_body = F.split(F.col("raw_ob"), r"\b(TEMPO|BECMG|NOSIG|RMK)\b")[0]
    trend_body = F.when(
        F.size(F.split(F.col("raw_ob"), r"\b(TEMPO|BECMG|NOSIG|RMK)\b")) > 1,
        F.element_at(F.split(F.col("raw_ob"), r"\b(TEMPO|BECMG|NOSIG|RMK)\b"), -1),
    ).otherwise(F.lit(""))

    # Visibility Extraction
    is_cavok = main_body.contains("CAVOK")
    vis_metric_token = F.regexp_extract(main_body, r"(?<!\S)(\d{4})(?!\S)", 1)

    vis_from_raw = (
        F.when(is_cavok | (vis_metric_token == "9999"), F.lit(10000))
        .when(
            (vis_metric_token != "") & (~vis_metric_token.startswith("0000")),
            vis_metric_token.cast("int"),
        )
        .when(
            (F.col("visib_raw") == "6+") | (F.col("visib_raw") == "10+"),
            F.lit(10000),
        )
        .when(
            F.col("visib_raw").isNotNull(),
            F.round(F.col("visib_raw").cast("double") * 1609.34).cast("int"),
        )
        .otherwise(F.lit(None))
    )

    vis_capped = is_cavok | (vis_from_raw >= 10000)
    is_low_vis = vis_from_raw.isNotNull() & (vis_from_raw < 5000)

    # Station weather: precipitation or thunderstorm (excluding VC* vicinity tokens)
    # Match standard tokens: -RA, +RA, RA, TSRA, +TSRA, -TSRA, SHRA, TS, DZ, SQ, etc.
    wx_token = F.regexp_extract(
        main_body,
        r"(?<!\S)((\+|-)?(TS|SH|FZ)?(RA|DZ|SN|GR|GS|PL|SQ|FC|TS)+)(?!\S)",
        1,
    )
    has_station_precip = (wx_token != "") & (~wx_token.startswith("VC"))
    station_wx_codes = F.when(
        has_station_precip, F.array(wx_token)
    ).otherwise(F.array().cast("array<string>"))

    # Vicinity weather (VCTS, VCSH, VCFG, etc.)
    vc_token = F.regexp_extract(main_body, r"(?<!\S)(VC(TS|SH|FG|RA|PO|BL|DS|SS)+)(?!\S)", 1)
    has_vc = vc_token != ""
    vc_wx_codes = F.when(has_vc, F.array(vc_token)).otherwise(F.array().cast("array<string>"))

    # Trend weather (e.g. TEMPO 3000 TSRA)
    trend_wx_token = F.regexp_extract(
        trend_body,
        r"(?<!\S)((\+|-)?(TS|SH|FZ)?(RA|DZ|SN|GR|GS|PL|SQ|FC|TS)+|VC(TS|SH|FG|RA)+)(?!\S)",
        1,
    )
    trend_wx_codes = F.when(
        trend_wx_token != "", F.array(trend_wx_token)
    ).otherwise(F.array().cast("array<string>"))

    return (
        df.withColumn("visibility_m", vis_from_raw)
        .withColumn("visibility_capped_9999", vis_capped)
        .withColumn("is_low_visibility", is_low_vis)
        .withColumn("has_precip_or_thunder_at_station", has_station_precip)
        .withColumn("station_weather_codes", station_wx_codes)
        .withColumn("has_vicinity_weather", has_vc)
        .withColumn("vicinity_weather_codes", vc_wx_codes)
        .withColumn("trend_weather_codes", trend_wx_codes)
        .withColumn("is_amended_or_speci", is_amended_or_speci)
    )


def enrich_valid_himawari(df: DataFrame) -> DataFrame:
    """Classifies B13 convective cloud categories and produces Vietnamese human-readable summary."""
    cond_severe = (F.col("inner_severe_convection_fraction") >= 0.05) | (
        F.col("inner_bt_min_k") < 220.0
    )
    cond_cold = (F.col("inner_cold_cloud_fraction") >= 0.05) | (F.col("inner_bt_min_k") < 235.0)
    cond_outer_cold = F.col("outer_cold_cloud_fraction") >= 0.05

    tag_col = (
        F.when(cond_severe, F.lit("SEVERE_CONVECTION_AT_STATION"))
        .when(cond_cold, F.lit("COLD_CONVECTIVE_CLOUD_AT_STATION"))
        .when(cond_outer_cold, F.lit("COLD_CLOUD_IN_OUTER_VICINITY"))
        .otherwise(F.lit("WARM_OR_CLEAR_CLOUD"))
    )

    summary_vi_col = (
        F.when(
            cond_severe,
            F.lit("Đỉnh mây dông phát triển rất mạnh (< -53.15°C) ngay trên sân bay"),
        )
        .when(
            cond_cold,
            F.lit("Có mây đối lưu lạnh (< -38.15°C) tại trạm"),
        )
        .when(
            cond_outer_cold,
            F.lit("Có dải mây đối lưu lạnh trong vùng lân cận ±1.0°"),
        )
        .otherwise(F.lit("Mây tầng thấp hoặc trời ít mây"))
    )

    return df.withColumn("b13_cloud_tag", tag_col).withColumn("b13_summary_vi", summary_vi_col)
