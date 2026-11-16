"""E5 rule/forecast verification -> Gold `verification` (one committed version per run).

CLI: ``python -m src.analytics.verification --storage-root data [--lake-root ...]``

Forecast skill (section `forecast_skill`)
-----------------------------------------
* Unit: (airport, forecast valid hour T).
* Forecast used: the newest Open-Meteo version with ``fetched_at_utc <= T - 1h`` - the version a
  reviewer had when T was the "next hour" - and never an `is_backfill` version.
* Forecast event: ``greatest(precipitation_mm, rain_mm + showers_mm) >= threshold`` (Level-3 formula);
  thresholds 0.1 / 0.5 / 1.0 mm for the POD-FAR sensitivity.
* Observed event: any METAR of that airport with ``observed_at_utc`` in (T - 1h, T] and
  `has_precip_or_thunder_at_station` (low visibility alone is NOT an event).
  The interval is open at T-1h (that report belongs to the previous hour) and closed at T.
* A unit is scored only when the observation window is judgeable: at least one METAR in the window and
  T not later than the newest METAR of the airport. Others are counted in `n_excluded_no_metar`.
* Metrics: 2x2 counts, POD, FAR, CSI, frequency bias. No Brier score (deterministic forecast).
  `event_rate` = observed base rate with its Wilson 95 % interval (`ci_low`, `ci_high`).
  `forecast_age_median_h` = median age of the forecast at issuance (T - 1h - fetched_at).

Level ordering (sections `level_event_rate*`, `level_monotonic*`)
-----------------------------------------------------------------
For every Gold decision row (replay + live slots) the event rate in the 60 minutes after the slot,
(slot, slot + 60'], grouped by priority level, with a Wilson 95 % interval. Two event definitions:
`level_event_rate` = precipitation/thunder at the station (same as the forecast verification) and
`level_event_rate_hazard` = precipitation/thunder OR visibility < 5 km (the E6 label). Rows are evaluated
only when the 60' window is fully observed and holds at least one METAR. `level_monotonic*` rows hold the
check rate(L1) >= rate(L3) >= rate(L4) (NULL when a level has no evaluable row); Level 2 is descriptive
only. Live slots are 5 minutes apart so their windows overlap: `n` counts decision rows, not independent
events, and the intervals are optimistic.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.ml.evaluate import scores_from_counts, wilson_interval
from src.ml.labels import hazard_flag, minutes
from src.spark.lake_paths import default_lake_root, new_run_id
from src.spark.priority_rules import FORECAST_RAIN_ALERT_MM
from src.spark.writers import read_table, write_table_version

THRESHOLDS_MM = (FORECAST_RAIN_ALERT_MM, 0.5, 1.0)
LEVELS = (1, 2, 3, 4)
MONOTONIC_LEVELS = (1, 3, 4)
ALL = "ALL"
_TS_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

SCHEMA: list[tuple[str, str]] = [
    ("section", "string"),
    ("airport_icao", "string"),
    ("threshold_mm", "double"),
    ("level", "int"),
    ("hits", "long"),
    ("misses", "long"),
    ("false_alarms", "long"),
    ("correct_negatives", "long"),
    ("pod", "double"),
    ("far", "double"),
    ("csi", "double"),
    ("frequency_bias", "double"),
    ("n_events", "long"),
    ("n", "long"),
    ("n_excluded_no_metar", "long"),
    ("window_start_utc", "timestamp"),
    ("window_end_utc", "timestamp"),
    ("event_rate", "double"),
    ("ci_low", "double"),
    ("ci_high", "double"),
    ("monotonic_ok", "boolean"),
    ("forecast_age_median_h", "double"),
    ("note", "string"),
    ("run_id", "string"),
]


def _forecast_rain() -> Column:
    return F.greatest(
        F.coalesce(F.col("precipitation_mm"), F.lit(0.0)),
        F.coalesce(F.col("rain_mm"), F.lit(0.0)) + F.coalesce(F.col("showers_mm"), F.lit(0.0)),
    )


def _observations(metar_df: DataFrame) -> DataFrame:
    return metar_df.select(
        F.col("airport_icao").alias("o_icao"),
        F.col("observed_at_utc").alias("o_at"),
        F.coalesce(F.col("has_precip_or_thunder_at_station"), F.lit(False)).cast("int").alias("o_precip"),
        hazard_flag().cast("int").alias("o_hazard"),
    )


def forecast_units(forecast_df: DataFrame, metar_df: DataFrame) -> DataFrame:
    """One row per (airport, valid hour T) whose observation window is complete.

    Columns: airport_icao, valid_at_utc, forecast_fetched_at_utc, forecast_rain_mm, forecast_age_h,
    n_obs, obs_event (0/1), evaluable (n_obs > 0).
    """
    fc = forecast_df.filter(~F.coalesce(F.col("is_backfill"), F.lit(False))).select(
        "airport_icao",
        F.col("forecast_valid_at_utc").alias("valid_at_utc"),
        F.col("fetched_at_utc").alias("forecast_fetched_at_utc"),
        _forecast_rain().alias("forecast_rain_mm"),
    )
    issued = fc.filter(F.col("forecast_fetched_at_utc") <= F.col("valid_at_utc") - minutes(60))
    w = Window.partitionBy("airport_icao", "valid_at_utc").orderBy(F.col("forecast_fetched_at_utc").desc())
    latest = issued.withColumn("_rk", F.row_number().over(w)).filter(F.col("_rk") == 1).drop("_rk")

    obs = _observations(metar_df)
    newest = obs.groupBy("o_icao").agg(F.max("o_at").alias("newest_obs_at"))
    window = (
        (F.col("airport_icao") == F.col("o_icao"))
        & (F.col("o_at") > F.col("valid_at_utc") - minutes(60))
        & (F.col("o_at") <= F.col("valid_at_utc"))
    )
    agg = (
        latest.join(obs, window, "left")
        .groupBy("airport_icao", "valid_at_utc", "forecast_fetched_at_utc", "forecast_rain_mm")
        .agg(F.count("o_at").alias("n_obs"), F.coalesce(F.max("o_precip"), F.lit(0)).alias("obs_event"))
    )
    return (
        agg.join(newest, F.col("airport_icao") == F.col("o_icao"))
        .filter(F.col("valid_at_utc") <= F.col("newest_obs_at"))
        .drop("o_icao", "newest_obs_at")
        .withColumn("evaluable", F.col("n_obs") > 0)
        .withColumn(
            "forecast_age_h",
            (F.unix_timestamp("valid_at_utc") - 3600 - F.unix_timestamp("forecast_fetched_at_utc")) / 3600.0,
        )
    )


def forecast_counts(units: DataFrame, thresholds: Sequence[float] = THRESHOLDS_MM) -> list[dict[str, Any]]:
    """2x2 counts per (airport, threshold) plus window bounds and median forecast age; list of dicts."""
    thr = units.sparkSession.range(1).select(
        F.explode(F.array(*[F.lit(float(t)) for t in thresholds])).alias("threshold_mm")
    )
    scored = units.crossJoin(thr)
    fe = F.col("forecast_rain_mm") >= F.col("threshold_mm")
    oe = F.col("obs_event") == 1
    ev = F.col("evaluable")
    counts = scored.groupBy("airport_icao", "threshold_mm").agg(
        F.sum(F.when(ev & fe & oe, 1).otherwise(0)).alias("hits"),
        F.sum(F.when(ev & ~fe & oe, 1).otherwise(0)).alias("misses"),
        F.sum(F.when(ev & fe & ~oe, 1).otherwise(0)).alias("false_alarms"),
        F.sum(F.when(ev & ~fe & ~oe, 1).otherwise(0)).alias("correct_negatives"),
        F.sum(F.when(~ev, 1).otherwise(0)).alias("n_excluded_no_metar"),
        F.date_format(F.min(F.when(ev, F.col("valid_at_utc"))), _TS_FMT).alias("window_start"),
        F.date_format(F.max(F.when(ev, F.col("valid_at_utc"))), _TS_FMT).alias("window_end"),
    )
    age = units.filter(F.col("evaluable")).groupBy("airport_icao").agg(
        F.percentile_approx("forecast_age_h", 0.5).alias("age_median_h")
    )
    out = []
    ages = {r["airport_icao"]: r["age_median_h"] for r in age.collect()}
    for r in counts.collect():
        d = r.asDict()
        d["age_median_h"] = ages.get(d["airport_icao"])
        out.append(d)
    return out


def level_units(decision_df: DataFrame, metar_df: DataFrame) -> DataFrame:
    """Per decision row: events in (slot, slot + 60'] (columns n_obs, precip_event, hazard_event, evaluable)."""
    slots = decision_df.select(
        "airport_icao", F.col("review_slot_utc").alias("slot_utc"), "priority_level"
    ).dropDuplicates()
    obs = _observations(metar_df)
    newest = obs.groupBy("o_icao").agg(F.max("o_at").alias("newest_obs_at"))
    window = (
        (F.col("airport_icao") == F.col("o_icao"))
        & (F.col("o_at") > F.col("slot_utc"))
        & (F.col("o_at") <= F.col("slot_utc") + minutes(60))
    )
    agg = (
        slots.join(obs, window, "left")
        .groupBy("airport_icao", "slot_utc", "priority_level")
        .agg(
            F.count("o_at").alias("n_obs"),
            F.coalesce(F.max("o_precip"), F.lit(0)).alias("precip_event"),
            F.coalesce(F.max("o_hazard"), F.lit(0)).alias("hazard_event"),
        )
    )
    return (
        agg.join(newest, F.col("airport_icao") == F.col("o_icao"))
        .filter(F.col("slot_utc") + minutes(60) <= F.col("newest_obs_at"))
        .drop("o_icao", "newest_obs_at")
        .withColumn("evaluable", F.col("n_obs") > 0)
    )


def level_counts(units: DataFrame) -> list[dict[str, Any]]:
    rows = (
        units.groupBy("airport_icao", "priority_level")
        .agg(
            F.sum(F.when(F.col("evaluable"), 1).otherwise(0)).alias("n"),
            F.sum(F.when(F.col("evaluable") & (F.col("precip_event") == 1), 1).otherwise(0)).alias("precip_events"),
            F.sum(F.when(F.col("evaluable") & (F.col("hazard_event") == 1), 1).otherwise(0)).alias("hazard_events"),
            F.sum(F.when(~F.col("evaluable"), 1).otherwise(0)).alias("n_excluded_no_metar"),
            F.date_format(F.min(F.when(F.col("evaluable"), F.col("slot_utc"))), _TS_FMT).alias("window_start"),
            F.date_format(F.max(F.when(F.col("evaluable"), F.col("slot_utc"))), _TS_FMT).alias("window_end"),
        )
        .collect()
    )
    return [r.asDict() for r in rows]


# --- Gold rows (plain Python; Spark only supplies the counts) ------------------------------------

def _row(section: str, **kw: Any) -> dict[str, Any]:
    row: dict[str, Any] = {name: None for name, _ in SCHEMA}
    row["section"] = section
    row.update(kw)
    return row


def _min_iso(values: list[str | None]) -> str | None:
    vals = [v for v in values if v]
    return min(vals) if vals else None


def _max_iso(values: list[str | None]) -> str | None:
    vals = [v for v in values if v]
    return max(vals) if vals else None


def skill_rows(counts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """`forecast_skill` rows: per airport and pooled (ALL), per threshold."""
    airports = sorted({c["airport_icao"] for c in counts})
    rows: list[dict[str, Any]] = []
    for scope in [*airports, ALL]:
        picked = [c for c in counts if scope == ALL or c["airport_icao"] == scope]
        for thr in sorted({c["threshold_mm"] for c in picked}):
            part = [c for c in picked if c["threshold_mm"] == thr]
            agg = {k: sum(int(c[k]) for c in part) for k in ("hits", "misses", "false_alarms", "correct_negatives", "n_excluded_no_metar")}
            sc = scores_from_counts(agg["hits"], agg["misses"], agg["false_alarms"], agg["correct_negatives"])
            n, events = sc["n"], sc["n_events"]
            lo, hi = wilson_interval(events, n)
            ages = [c["age_median_h"] for c in part if c.get("age_median_h") is not None]
            rows.append(
                _row(
                    "forecast_skill",
                    airport_icao=scope,
                    threshold_mm=float(thr),
                    **agg,
                    pod=sc["pod"], far=sc["far"], csi=sc["csi"], frequency_bias=sc["frequency_bias"],
                    n_events=events, n=n,
                    window_start_utc=_min_iso([c["window_start"] for c in part]),
                    window_end_utc=_max_iso([c["window_end"] for c in part]),
                    event_rate=events / n if n else None, ci_low=lo, ci_high=hi,
                    forecast_age_median_h=(sorted(ages)[len(ages) // 2] if ages else None),
                    note="event_rate = observed precip/thunder base rate; CI = Wilson 95 %",
                )
            )
    return rows


def _monotonic(rates: dict[int, float | None]) -> tuple[bool | None, str]:
    text = ", ".join(f"L{lv}={rates[lv]:.3f}" if rates.get(lv) is not None else f"L{lv}=n/a" for lv in MONOTONIC_LEVELS)
    if any(rates.get(lv) is None for lv in MONOTONIC_LEVELS):
        return None, f"not evaluable (a level has no evaluable decision row): {text}"
    ok = rates[1] >= rates[3] >= rates[4]
    return ok, f"L1 >= L3 >= L4 {'holds' if ok else 'is violated'}: {text}; L2 descriptive only"


def level_rows(counts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    airports = sorted({c["airport_icao"] for c in counts})
    rows: list[dict[str, Any]] = []
    overlap = "decision rows are 5' apart and their 60' windows overlap: CI is optimistic"
    for suffix, key in (("", "precip_events"), ("_hazard", "hazard_events")):
        for scope in [ALL, *airports]:
            rates: dict[int, float | None] = {}
            for lv in LEVELS:
                part = [c for c in counts if c["priority_level"] == lv and (scope == ALL or c["airport_icao"] == scope)]
                n = sum(int(c["n"]) for c in part)
                events = sum(int(c[key]) for c in part)
                excluded = sum(int(c["n_excluded_no_metar"]) for c in part)
                lo, hi = wilson_interval(events, n)
                rate = events / n if n else None
                rates[lv] = rate
                rows.append(
                    _row(
                        f"level_event_rate{suffix}",
                        airport_icao=scope, level=lv, n_events=events, n=n, n_excluded_no_metar=excluded,
                        window_start_utc=_min_iso([c["window_start"] for c in part]),
                        window_end_utc=_max_iso([c["window_end"] for c in part]),
                        event_rate=rate, ci_low=lo, ci_high=hi,
                        note=overlap + ("; Level 2 descriptive only" if lv == 2 else ""),
                    )
                )
            ok, note = _monotonic(rates)
            rows.append(_row(f"level_monotonic{suffix}", airport_icao=scope, monotonic_ok=ok, note=note))
    return rows


def literal_frame(spark: SparkSession, rows: list[dict[str, Any]], schema: list[tuple[str, str]], run_id: str) -> DataFrame:
    """DataFrame of literals built in the JVM (no Python RDD): inline(array(struct(...)))."""
    structs = []
    for row in rows:
        cols = []
        for name, dtype in schema:
            value = run_id if name == "run_id" else row.get(name)
            if value is None:
                col = F.lit(None).cast(dtype)
            elif dtype == "timestamp":
                col = F.to_timestamp(F.lit(value))
            else:
                col = F.lit(value).cast(dtype)
            cols.append(col.alias(name))
        structs.append(F.struct(*cols))
    return spark.range(1).select(F.inline(F.array(*structs)))


def run_verification(spark: SparkSession, lake_root: str) -> dict[str, Any]:
    metar = read_table(spark, lake_root, "metar_observations")
    forecast = read_table(spark, lake_root, "open_meteo_forecasts")
    decision = read_table(spark, lake_root, "decision_table")
    missing = [n for n, d in (("metar_observations", metar), ("open_meteo_forecasts", forecast), ("decision_table", decision)) if d is None]
    if missing:
        raise SystemExit(f"Lake tables without a committed version: {missing}. Run src.spark.run_pipeline first.")

    units = forecast_units(forecast, metar).cache()
    skill = skill_rows(forecast_counts(units))
    units.unpersist()
    levels = level_rows(level_counts(level_units(decision, metar)))
    rows = [*skill, *levels]

    run_id = new_run_id()
    write_table_version(literal_frame(spark, rows, SCHEMA, run_id), lake_root, "verification", run_id)
    return {"run_id": run_id, "lake_root": lake_root, "rows": len(rows), "table": rows}


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Verify the Level 1-4 rules and Open-Meteo rain forecast (Gold verification)")
    parser.add_argument("--storage-root", default="data")
    parser.add_argument("--lake-root", default=None)
    args = parser.parse_args(argv)
    from src.spark.session import get_spark_session

    spark = get_spark_session("AirportVerification")
    lake = args.lake_root or default_lake_root(args.storage_root)
    result = run_verification(spark, lake)
    print(json.dumps({k: v for k, v in result.items() if k != "table"}, indent=2))
    for row in result["table"]:
        if row["section"] in ("forecast_skill", "level_event_rate", "level_event_rate_hazard", "level_monotonic", "level_monotonic_hazard") and row["airport_icao"] == ALL:
            print(json.dumps({k: v for k, v in row.items() if v is not None}, ensure_ascii=False))


if __name__ == "__main__":
    main()
