"""E6 labels for the 0-60 minute nowcast.

Label of a sample time `t` (per airport):

    y = 1  if any METAR/SPECI with observed_at_utc in (t, t + 60'] reports
              precipitation/thunder at the station  OR  visibility_m < 5000
    y = 0  if the window holds at least one METAR and none of them does

The window is open at `t` (an observation exactly at `t` is the *current* report and belongs to the
features, never to the label) and closed at `t + 60'`.

The label is **unknown** (`label_known = false`, `y = null`) when the window cannot be judged:
no METAR falls inside it (data gap) or it reaches past the newest METAR of the airport (the future is
not observed yet). Unknown rows are kept in the feature table (inference needs them) but never used
for training or evaluation.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.spark.priority_rules import LOW_VISIBILITY_M

LABEL_HORIZON_MIN = 60


def minutes(n: int) -> Column:
    """Interval literal of `n` minutes."""
    return F.expr(f"INTERVAL {int(n)} MINUTES")


def hazard_flag(
    precip_col: str = "has_precip_or_thunder_at_station", visibility_col: str = "visibility_m"
) -> Column:
    """Boolean: precip/thunder at the station OR visibility below 5 km (null inputs count as false)."""
    precip = F.coalesce(F.col(precip_col), F.lit(False))
    low_vis = F.coalesce(F.col(visibility_col) < F.lit(LOW_VISIBILITY_M), F.lit(False))
    return precip | low_vis


def attach_labels(samples_df: DataFrame, metar_df: DataFrame) -> DataFrame:
    """Adds `label_n_obs`, `label_known` and `y` to `samples_df`.

    samples_df: one row per (`airport_icao`, `sample_time_utc`).
    metar_df:   Silver METAR observations (`airport_icao`, `observed_at_utc`, flags, `visibility_m`).
    """
    obs = metar_df.select(
        F.col("airport_icao").alias("o_icao"),
        F.col("observed_at_utc").alias("o_at"),
        hazard_flag().cast("int").alias("o_hazard"),
    )
    newest = obs.groupBy("o_icao").agg(F.max("o_at").alias("newest_obs_at"))
    window = (
        (F.col("airport_icao") == F.col("o_icao"))
        & (F.col("o_at") > F.col("sample_time_utc"))
        & (F.col("o_at") <= F.col("sample_time_utc") + minutes(LABEL_HORIZON_MIN))
    )
    agg = (
        samples_df.join(obs, window, "left")
        .groupBy("airport_icao", "sample_time_utc")
        .agg(F.count("o_at").alias("label_n_obs"), F.max("o_hazard").alias("_hazard"))
    )
    out = agg.join(newest, F.col("airport_icao") == F.col("o_icao"), "left").drop("o_icao")
    known = (F.col("label_n_obs") > 0) & (
        F.col("sample_time_utc") + minutes(LABEL_HORIZON_MIN) <= F.col("newest_obs_at")
    )
    return (
        out.withColumn("label_known", known)
        .withColumn("y", F.when(F.col("label_known"), F.coalesce(F.col("_hazard"), F.lit(0))).cast("int"))
        .drop("_hazard", "newest_obs_at")
    )
