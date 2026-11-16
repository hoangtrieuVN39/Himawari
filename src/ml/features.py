"""E6 feature table for the 0-60 minute nowcast (Gold `feature_table`, partitioned by `date`).

CLI: ``python -m src.ml.features --storage-root data [--lake-root ...]``

Sample times
------------
Training rows use `t` = every METAR/SPECI observation time of every airport. Inference (src.ml.infer)
calls `build_features` with arbitrary review slots. Both share the same code path, so a feature can
never mean something different at training and at serving time.

Point-in-time rule (no future leakage)
--------------------------------------
Every input of a row must satisfy ``available_at <= t``:

* METAR       - ``observed_at_utc <= t`` (event time; `availability="fetched"`, used by inference,
                additionally needs ``fetched_at_utc <= t`` unless the report is a back-fill).
* Open-Meteo  - the newest forecast version with ``fetched_at_utc <= t``. Back-filled forecasts are
                fetched after `t` by construction and therefore never qualify. A missing forecast stays
                missing (`forecast_missing = 1`); it is never filled from a later version.
* Himawari B13 - ``scan_time_utc <= t`` (and, for the lookup of the *latest* scan, within 60' of `t`).
                `scan_time` is the observation time; the product reaches the lake a few minutes later,
                so this is slightly optimistic for real-time use (documented, as agreed in the plan).
Provenance columns (`cur_observed_at_utc`, `fc_fetched_at_utc`, `b13_scan_time_utc`, ...) are kept in the
table so the rule can be audited and unit-tested.

Open-Meteo window mismatch (read before interpreting `fc_rain_next_hour_mm`)
---------------------------------------------------------------------------
Open-Meteo hourly `precipitation`/`rain`/`showers` are sums over the hour *preceding* the valid time
(to be confirmed against the provider documentation, plan section 1.2 item 7). The "next hour" feature
therefore uses the value valid at ``floor(t) + 1h``, which covers ``(floor(t), floor(t) + 1h]``. The
label covers ``(t, t + 60']``. The two windows only coincide when `t` falls exactly on the hour; at
`t = hh:30` half of the forecast hour has already elapsed and the label window reaches 30' into the
following hour. The model must learn that offset; the dashboard rule (Level 3) and the E5 verification
use the same ``floor(t) + 1h`` lookup, so the three stay comparable.

Cloud-top cooling
-----------------
`bt_delta_15m_c` / `bt_delta_30m_c` = `inner_bt_min_c` of the latest scan L minus that of an earlier
scan: the latest scan within ``(L - k - 10', L - k]`` (10' = B13 cadence). On the 10' grid this is the scan
at L-20' for k=15 and at L-30' for k=30. Negative = cooling. If no scan exists in that band the
delta is null; values are never interpolated across gaps.
"""

from __future__ import annotations

import argparse
import json
import math
from typing import Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ml.labels import attach_labels, hazard_flag, minutes
from src.spark.lake_paths import default_lake_root, new_run_id
from src.spark.priority_rules import FORECAST_RAIN_ALERT_MM, METAR_STALE_MINUTES
from src.spark.writers import read_table, write_table_version

AIRPORTS = ["VVDN", "VVNB", "VVTS"]
B13_CADENCE_MIN = 10
B13_MAX_AGE_MIN = 60
COOLING_HORIZONS_MIN = (15, 30)
METAR_LOOKBACK_MIN = 180
ICT_TZ = "Asia/Ho_Chi_Minh"

KEY = ["airport_icao", "sample_time_utc"]

# Columns the model reads (after `prepare_model_frame`). Order is the vector order.
MODEL_FEATURES: list[str] = [
    "cur_precip_or_thunder",
    "cur_low_visibility",
    "cur_visibility_km",
    "cur_vicinity_weather",
    "pos_obs_1h",
    "pos_obs_3h",
    "fc_rain_next_hour_mm",
    "forecast_missing",
    "b13_missing",
    "inner_bt_min_c",
    "inner_cold_cloud_fraction",
    "inner_severe_convection_fraction",
    "bt_delta_15m_c",
    "bt_delta_30m_c",
    "bt_delta_15m_missing",
    "bt_delta_30m_missing",
    "hour_sin",
    "hour_cos",
    *[f"airport_{a}" for a in AIRPORTS],
]

FEATURE_LABELS_VI: dict[str, str] = {
    "cur_precip_or_thunder": "METAR hiện tại có mưa/dông",
    "cur_low_visibility": "METAR hiện tại tầm nhìn dưới 5 km",
    "cur_visibility_km": "Tầm nhìn hiện tại",
    "cur_vicinity_weather": "Hiện tượng thời tiết lân cận sân bay",
    "pos_obs_1h": "Số bản tin nguy hiểm trong 1 giờ qua",
    "pos_obs_3h": "Số bản tin nguy hiểm trong 3 giờ qua",
    "fc_rain_next_hour_mm": "Dự báo mưa giờ tới (Open-Meteo)",
    "forecast_missing": "Thiếu dự báo Open-Meteo",
    "b13_missing": "Thiếu ảnh vệ tinh B13",
    "inner_bt_min_c": "Nhiệt độ sáng đỉnh mây thấp nhất (B13)",
    "inner_cold_cloud_fraction": "Tỷ lệ mây lạnh (B13)",
    "inner_severe_convection_fraction": "Tỷ lệ đối lưu mạnh (B13)",
    "bt_delta_15m_c": "Đỉnh mây lạnh đi trong 15 phút (B13)",
    "bt_delta_30m_c": "Đỉnh mây lạnh đi trong 30 phút (B13)",
    "bt_delta_15m_missing": "Thiếu lượt quét B13 15 phút trước",
    "bt_delta_30m_missing": "Thiếu lượt quét B13 30 phút trước",
    "hour_sin": "Giờ trong ngày (sin)",
    "hour_cos": "Giờ trong ngày (cos)",
    "airport_VVDN": "Sân bay Đà Nẵng (VVDN)",
    "airport_VVNB": "Sân bay Nội Bài (VVNB)",
    "airport_VVTS": "Sân bay Tân Sơn Nhất (VVTS)",
}

# Neutral values for null inputs; `inner_bt_min_c` is replaced by the training mean (see fit_fill_values).
FILL_CONSTANTS: dict[str, float] = {
    "cur_visibility_km": 10.0,
    "fc_rain_next_hour_mm": 0.0,
    "inner_bt_min_c": 0.0,
    "inner_cold_cloud_fraction": 0.0,
    "inner_severe_convection_fraction": 0.0,
    "bt_delta_15m_c": 0.0,
    "bt_delta_30m_c": 0.0,
}


def _forecast_rain_expr() -> Column:
    """Same formula as the Level-3 rule (temporal_join): greatest(precipitation, rain + showers)."""
    return F.greatest(
        F.coalesce(F.col("precipitation_mm"), F.lit(0.0)),
        F.coalesce(F.col("rain_mm"), F.lit(0.0)) + F.coalesce(F.col("showers_mm"), F.lit(0.0)),
    )


def sample_times_from_metar(metar_df: DataFrame) -> DataFrame:
    """Training sample times: one row per (airport, METAR observation time).

    `obs_is_backfill` is true only when every record of that observation was back-filled.
    """
    return (
        metar_df.groupBy("airport_icao", F.col("observed_at_utc").alias("sample_time_utc"))
        .agg(F.min(F.col("is_backfill").cast("int")).alias("_bf"))
        .withColumn("obs_is_backfill", F.col("_bf") == 1)
        .drop("_bf")
    )


def _metar_block(samples: DataFrame, metar_df: DataFrame, availability: str) -> DataFrame:
    m = metar_df.select(
        F.col("airport_icao").alias("m_icao"),
        F.col("observed_at_utc").alias("m_at"),
        F.col("fetched_at_utc").alias("m_fetched"),
        F.col("is_backfill").alias("m_bf"),
        F.coalesce(F.col("revision_index"), F.lit(0)).alias("m_rev"),
        F.col("visibility_m").alias("m_vis"),
        F.coalesce(F.col("has_precip_or_thunder_at_station"), F.lit(False)).alias("m_precip"),
        F.coalesce(F.col("has_vicinity_weather"), F.lit(False)).alias("m_vicinity"),
        hazard_flag().alias("m_hazard"),
    )
    t = F.col("sample_time_utc")
    cond = (
        (F.col("airport_icao") == F.col("m_icao"))
        & (F.col("m_at") <= t)
        & (F.col("m_at") > t - minutes(METAR_LOOKBACK_MIN))
    )
    if availability == "fetched":
        cond = cond & ((F.col("m_fetched") <= t) | F.coalesce(F.col("m_bf"), F.lit(False)))
    elif availability != "event_time":
        raise ValueError(f"availability must be 'event_time' or 'fetched', got {availability!r}")
    return (
        samples.join(m, cond, "left")
        .groupBy(*KEY)
        .agg(
            F.max(F.struct("m_at", "m_rev", "m_vis", "m_precip", "m_vicinity", "m_hazard")).alias("cur"),
            F.sum(F.when(F.col("m_hazard") & (F.col("m_at") > t - minutes(60)), 1).otherwise(0)).alias(
                "pos_obs_1h"
            ),
            F.sum(F.when(F.col("m_hazard"), 1).otherwise(0)).alias("pos_obs_3h"),
        )
    )


def _forecast_block(samples: DataFrame, forecast_df: DataFrame) -> DataFrame:
    f = forecast_df.select(
        F.col("airport_icao").alias("f_icao"),
        F.col("fetched_at_utc").alias("f_fetched"),
        F.col("forecast_valid_at_utc").alias("f_valid"),
        _forecast_rain_expr().alias("f_rain"),
    )
    t = F.col("sample_time_utc")
    cond = (
        (F.col("airport_icao") == F.col("f_icao"))
        & (F.col("f_valid") == F.date_trunc("hour", t) + minutes(60))
        & (F.col("f_fetched") <= t)
    )
    return samples.join(f, cond, "left").groupBy(*KEY).agg(F.max(F.struct("f_fetched", "f_rain")).alias("fc"))


def _b13_blocks(samples: DataFrame, b13_df: DataFrame, availability: str) -> list[DataFrame]:
    """[latest-scan block, one 'earlier scan' block per cooling horizon], each keyed by KEY."""
    synthetic = F.coalesce(F.col("is_synthetic_test"), F.lit(False)) if "is_synthetic_test" in b13_df.columns else F.lit(False)
    status_ok = (F.col("status") == "AVAILABLE") if "status" in b13_df.columns else F.lit(True)
    b = b13_df.filter(~synthetic & status_ok & F.col("inner_bt_min_c").isNotNull()).select(
        F.col("airport_icao").alias("b_icao"),
        F.col("scan_time_utc").alias("b_scan"),
        F.col("fetched_at_utc").alias("b_fetched"),
        F.col("is_backfill").alias("b_bf"),
        F.col("inner_bt_min_c").alias("b_bt"),
        F.col("inner_cold_cloud_fraction").alias("b_cold"),
        F.col("inner_severe_convection_fraction").alias("b_severe"),
        F.col("b13_valid_fraction").alias("b_valid"),
    )
    t = F.col("sample_time_utc")
    reach = B13_MAX_AGE_MIN + max(COOLING_HORIZONS_MIN) + B13_CADENCE_MIN
    cond = (
        (F.col("airport_icao") == F.col("b_icao"))
        & (F.col("b_scan") <= t)
        & (F.col("b_scan") > t - minutes(reach))
    )
    if availability == "fetched":
        cond = cond & ((F.col("b_fetched") <= t) | F.coalesce(F.col("b_bf"), F.lit(False)))
    cand = samples.join(b, cond, "inner").select(
        *KEY, "b_scan", "b_bt", "b_cold", "b_severe", "b_valid"
    )
    latest = (
        cand.filter(F.col("b_scan") > t - minutes(B13_MAX_AGE_MIN))
        .groupBy(*KEY)
        .agg(F.max(F.struct("b_scan", "b_bt", "b_cold", "b_severe", "b_valid")).alias("latest"))
    )
    latest_time = latest.select(*KEY, F.col("latest.b_scan").alias("l_scan"))
    blocks = [latest]
    for k in COOLING_HORIZONS_MIN:
        prev = (
            cand.select(*KEY, F.col("b_scan").alias("p_scan"), F.col("b_bt").alias("p_bt"))
            .join(latest_time, KEY)
            .filter(
                (F.col("p_scan") <= F.col("l_scan") - minutes(k))
                & (F.col("p_scan") > F.col("l_scan") - minutes(k + B13_CADENCE_MIN))
            )
            .groupBy(*KEY)
            .agg(F.max(F.struct("p_scan", "p_bt")).alias(f"prev{k}"))
        )
        blocks.append(prev)
    return blocks


def build_features(
    samples_df: DataFrame,
    metar_df: DataFrame,
    forecast_df: DataFrame,
    b13_df: DataFrame,
    availability: str = "event_time",
) -> DataFrame:
    """Features of every (`airport_icao`, `sample_time_utc`) row of `samples_df` (extra columns are kept).

    All sources are Silver tables. See the module docstring for the point-in-time rules.
    """
    samples = samples_df.dropDuplicates(KEY)
    t = F.col("sample_time_utc")

    out = samples.join(_metar_block(samples, metar_df, availability), KEY, "left")
    out = out.join(_forecast_block(samples, forecast_df), KEY, "left")
    latest, prev15, prev30 = _b13_blocks(samples, b13_df, availability)
    out = out.join(latest, KEY, "left").join(prev15, KEY, "left").join(prev30, KEY, "left")

    ict = F.from_utc_timestamp(t, ICT_TZ)
    hour_frac = F.hour(ict) + F.minute(ict) / F.lit(60.0)
    angle = hour_frac * F.lit(2.0 * math.pi / 24.0)

    cols: list[Column] = [
        F.col("cur.m_at").alias("cur_observed_at_utc"),
        F.col("cur.m_at").isNull().cast("double").alias("metar_missing"),
        ((F.unix_timestamp(t) - F.unix_timestamp(F.col("cur.m_at"))) / 60.0).alias("metar_age_min"),
        F.coalesce(F.col("cur.m_precip"), F.lit(False)).cast("double").alias("cur_precip_or_thunder"),
        F.coalesce(F.col("cur.m_vis") < F.lit(5000), F.lit(False)).cast("double").alias("cur_low_visibility"),
        (F.col("cur.m_vis") / 1000.0).alias("cur_visibility_km"),
        F.coalesce(F.col("cur.m_vicinity"), F.lit(False)).cast("double").alias("cur_vicinity_weather"),
        F.coalesce(F.col("cur.m_hazard"), F.lit(False)).cast("double").alias("cur_hazard"),
        F.col("pos_obs_1h").cast("double"),
        F.col("pos_obs_3h").cast("double"),
        F.col("fc.f_fetched").alias("fc_fetched_at_utc"),
        F.col("fc.f_rain").alias("fc_rain_next_hour_mm"),
        F.col("fc.f_fetched").isNull().cast("double").alias("forecast_missing"),
        F.col("latest.b_scan").alias("b13_scan_time_utc"),
        F.col("latest.b_scan").isNull().cast("double").alias("b13_missing"),
        ((F.unix_timestamp(t) - F.unix_timestamp(F.col("latest.b_scan"))) / 60.0).alias("b13_age_min"),
        F.col("latest.b_bt").alias("inner_bt_min_c"),
        F.col("latest.b_cold").alias("inner_cold_cloud_fraction"),
        F.col("latest.b_severe").alias("inner_severe_convection_fraction"),
        F.col("latest.b_valid").alias("b13_valid_fraction"),
        F.col("prev15.p_scan").alias("b13_prev15_scan_time_utc"),
        F.col("prev30.p_scan").alias("b13_prev30_scan_time_utc"),
        (F.col("latest.b_bt") - F.col("prev15.p_bt")).alias("bt_delta_15m_c"),
        (F.col("latest.b_bt") - F.col("prev30.p_bt")).alias("bt_delta_30m_c"),
        F.sin(angle).alias("hour_sin"),
        F.cos(angle).alias("hour_cos"),
        *[(F.col("airport_icao") == a).cast("double").alias(f"airport_{a}") for a in AIRPORTS],
    ]
    extra = [c for c in samples.columns if c not in KEY]
    return out.select(*KEY, *extra, *cols)


def add_baselines(df: DataFrame) -> DataFrame:
    """Deterministic comparison rules evaluated at `t`, as 0/1 integer columns.

    * bl_persistence - the current METAR already meets the Level-1 condition (fresh and hazardous).
    * bl_open_meteo  - Open-Meteo next-hour rain >= 0.1 mm (missing forecast = no alert).
    * bl_rule        - the dashboard rule at `t` raises Level 1 or Level 3 (parity with
                       priority_rules.evaluate_priority is unit-tested).
    """
    fresh = F.coalesce(
        (F.col("metar_missing") == 0) & (F.col("metar_age_min") <= F.lit(METAR_STALE_MINUTES)), F.lit(False)
    )
    hazard = F.col("cur_hazard") == 1
    rain_alert = (F.col("forecast_missing") == 0) & (
        F.coalesce(F.col("fc_rain_next_hour_mm"), F.lit(0.0)) >= F.lit(FORECAST_RAIN_ALERT_MM)
    )
    return (
        df.withColumn("bl_persistence", (fresh & hazard).cast("int"))
        .withColumn(
            "bl_open_meteo",
            (F.coalesce(F.col("fc_rain_next_hour_mm"), F.lit(0.0)) >= F.lit(FORECAST_RAIN_ALERT_MM)).cast("int"),
        )
        .withColumn("bl_rule", (fresh & (hazard | rain_alert)).cast("int"))
    )


def fit_fill_values(train_df: DataFrame) -> dict[str, float]:
    """Null-replacement values: constants, except `inner_bt_min_c` = mean over the training rows."""
    fill = dict(FILL_CONSTANTS)
    mean_bt = train_df.agg(F.avg("inner_bt_min_c")).first()[0]
    if mean_bt is not None:
        fill["inner_bt_min_c"] = float(mean_bt)
    return fill


def prepare_model_frame(df: DataFrame, fill_values: dict[str, float]) -> DataFrame:
    """Adds the missing-delta indicators, fills nulls and casts every model feature to double."""
    out = (
        df.withColumn("bt_delta_15m_missing", F.col("bt_delta_15m_c").isNull().cast("double"))
        .withColumn("bt_delta_30m_missing", F.col("bt_delta_30m_c").isNull().cast("double"))
    )
    for name, value in fill_values.items():
        out = out.withColumn(name, F.coalesce(F.col(name), F.lit(float(value))))
    return out.select(*[c for c in out.columns if c not in MODEL_FEATURES], *[F.col(c).cast("double") for c in MODEL_FEATURES])


def build_feature_table(metar_df: DataFrame, forecast_df: DataFrame, b13_df: DataFrame) -> DataFrame:
    """Gold feature table: features + label + baselines for every METAR observation time."""
    samples = sample_times_from_metar(metar_df)
    features = build_features(samples, metar_df, forecast_df, b13_df, availability="event_time")
    out = features.join(attach_labels(samples, metar_df), KEY, "left")
    out = add_baselines(out)
    return out.withColumn("date", F.to_date("sample_time_utc"))


def run_feature_table(spark: SparkSession, lake_root: str) -> dict:
    metar = read_table(spark, lake_root, "metar_observations")
    forecast = read_table(spark, lake_root, "open_meteo_forecasts")
    b13 = read_table(spark, lake_root, "himawari_b13_metrics")
    missing = [n for n, d in (("metar_observations", metar), ("open_meteo_forecasts", forecast), ("himawari_b13_metrics", b13)) if d is None]
    if missing:
        raise SystemExit(f"Silver tables without a committed version: {missing}. Run src.spark.run_pipeline first.")
    table = build_feature_table(metar, forecast, b13).cache()
    run_id = new_run_id()
    write_table_version(table, lake_root, "feature_table", run_id, ["date"])
    stats = table.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("label_known").cast("int")).alias("labelled"),
        F.sum(F.when(F.col("y") == 1, 1).otherwise(0)).alias("positives"),
        F.sum(1 - F.col("forecast_missing").cast("int")).alias("with_forecast"),
        F.sum(1 - F.col("b13_missing").cast("int")).alias("with_b13"),
        F.date_format(F.min("sample_time_utc"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("first_t"),
        F.date_format(F.max("sample_time_utc"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("last_t"),
    ).first()
    table.unpersist()
    return {"run_id": run_id, "lake_root": lake_root, **stats.asDict()}


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build the Gold feature_table (features + 0-60' label)")
    parser.add_argument("--storage-root", default="data")
    parser.add_argument("--lake-root", default=None)
    args = parser.parse_args(argv)
    from src.spark.session import get_spark_session

    spark = get_spark_session("AirportNowcastFeatures")
    lake = args.lake_root or default_lake_root(args.storage_root)
    print(json.dumps(run_feature_table(spark, lake), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
