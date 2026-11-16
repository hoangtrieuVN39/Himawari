"""E6: label boundaries, point-in-time feature rules, cloud-top cooling deltas, rule baseline parity."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from src.ml.features import (
    KEY, MODEL_FEATURES, add_baselines, build_features, fit_fill_values, prepare_model_frame,
)
from src.ml.labels import attach_labels
from src.spark.priority_rules import PriorityInput, evaluate_priority


def fmt(col):
    return F.date_format(col, "yyyy-MM-dd HH:mm")


def _rows(df):
    return {r["airport_icao"]: r.asDict() for r in df.collect()}


# --- labels -----------------------------------------------------------------------------------

def test_label_window_is_open_at_t_and_closed_at_t_plus_60(spark, frames):
    metar = frames.metar([
        # A: the only hazard is the current report at t=10:00 -> not in (t, t+60'] -> y=0
        ("A", "10:00", True, 9999), ("A", "10:30", False, 9999), ("A", "11:00", False, 9999),
        # B: hazard exactly at t+60' -> included -> y=1
        ("B", "10:00", False, 9999), ("B", "10:30", False, 9999), ("B", "11:00", True, 9999),
        # C: hazard one minute after t+60' -> excluded -> y=0 (window still has 10:30 and 11:00)
        ("C", "10:00", False, 9999), ("C", "10:30", False, 9999), ("C", "11:00", False, 9999),
        ("C", "11:01", True, 9999),
    ])
    out = _rows(attach_labels(frames.samples([("A", "10:00"), ("B", "10:00"), ("C", "10:00")]), metar))
    assert (out["A"]["y"], out["A"]["label_known"], out["A"]["label_n_obs"]) == (0, True, 2)
    assert (out["B"]["y"], out["B"]["label_known"]) == (1, True)
    assert (out["C"]["y"], out["C"]["label_known"]) == (0, True)


def test_label_counts_low_visibility_but_not_9999_and_marks_unobserved_future_unknown(spark, frames):
    metar = frames.metar([
        ("LOW", "10:00", False, 9999), ("LOW", "10:30", False, 4999), ("LOW", "11:00", False, 9999),
        ("EDGE", "10:00", False, 9999), ("EDGE", "10:30", False, 5000), ("EDGE", "11:00", False, 9999),
        # FUT: the window reaches past the newest METAR (11:00 < t+60' = 11:30) -> unknown
        ("FUT", "10:30", False, 9999), ("FUT", "11:00", True, 9999),
        # GAP: no METAR at all in the window -> unknown
        ("GAP", "10:00", False, 9999), ("GAP", "12:30", False, 9999),
    ])
    out = _rows(attach_labels(
        frames.samples([("LOW", "10:00"), ("EDGE", "10:00"), ("FUT", "10:30"), ("GAP", "10:00")]), metar
    ))
    assert out["LOW"]["y"] == 1 and out["EDGE"]["y"] == 0
    assert out["FUT"]["label_known"] is False and out["FUT"]["y"] is None
    assert out["GAP"]["label_known"] is False and out["GAP"]["y"] is None


# --- future leakage -----------------------------------------------------------------------------

def _leak_inputs(frames):
    metar = frames.metar([
        ("VVDN", "09:00", False, 9999), ("VVDN", "09:30", True, 9999), ("VVDN", "10:00", False, 9000),
        ("VVDN", "10:10", True, 3000), ("VVDN", "10:30", True, 3000),  # future for t=10:00
    ])
    forecast = frames.forecast([
        ("VVDN", "09:00", "11:00", 0.0, 0.0, 0.0),
        ("VVDN", "10:00", "11:00", 0.3, 0.0, 0.0),   # fetched exactly at t -> usable
        ("VVDN", "10:01", "11:00", 5.0, 0.0, 0.0),   # fetched after t -> future
        ("VVDN", "09:00", "12:00", 9.0, 0.0, 0.0),   # valid hour floor(t)+2h -> wrong hour
    ])
    b13 = frames.b13([
        ("VVDN", "09:50", -40.0), ("VVDN", "10:00", -50.0), ("VVDN", "10:10", -70.0),
    ])
    return metar, forecast, b13


def test_features_never_use_data_after_t(spark, frames):
    metar, forecast, b13 = _leak_inputs(frames)
    samples = frames.samples([("VVDN", "09:30"), ("VVDN", "10:00")])
    feat = build_features(samples, metar, forecast, b13)
    rows = {r["h"]: r.asDict() for r in feat.withColumn("h", fmt("sample_time_utc")).collect()}
    r = rows["2026-11-10 10:00"]
    assert r["fc_rain_next_hour_mm"] == pytest.approx(0.3)           # not 5.0, not 9.0
    assert r["inner_bt_min_c"] == -50.0                              # not the 10:10 scan
    assert r["cur_visibility_km"] == pytest.approx(9.0)              # METAR at 10:00, not 10:10/10:30
    assert r["cur_precip_or_thunder"] == 0.0 and r["metar_age_min"] == 0.0
    assert r["pos_obs_1h"] == 1.0 and r["pos_obs_3h"] == 1.0         # only the 09:30 hazard; 10:10+ ignored
    early = rows["2026-11-10 09:30"]
    assert early["forecast_missing"] == 1.0 and early["fc_rain_next_hour_mm"] is None
    assert early["inner_bt_min_c"] is None and early["b13_missing"] == 1.0

    # every provenance timestamp is <= t, for every row
    violations = feat.filter(
        (F.col("cur_observed_at_utc") > F.col("sample_time_utc"))
        | (F.col("fc_fetched_at_utc") > F.col("sample_time_utc"))
        | (F.col("b13_scan_time_utc") > F.col("sample_time_utc"))
        | (F.col("b13_prev15_scan_time_utc") > F.col("sample_time_utc"))
        | (F.col("b13_prev30_scan_time_utc") > F.col("sample_time_utc"))
    ).count()
    assert violations == 0


def test_fetched_availability_hides_metar_not_yet_fetched_unless_backfill(spark, frames):
    metar = frames.metar([
        ("LIVE", "09:30", False, 9999, "09:35"),
        ("LIVE", "10:00", True, 3000, "10:20"),                 # observed <= t but fetched after t
        ("BACK", "10:00", True, 3000, "12:00", True),           # back-filled -> accepted in replay
    ])
    empty_f, empty_b = frames.forecast([]), frames.b13([])
    samples = frames.samples([("LIVE", "10:05"), ("BACK", "10:05")])
    fetched = _rows(build_features(samples, metar, empty_f, empty_b, availability="fetched"))
    assert fetched["LIVE"]["cur_precip_or_thunder"] == 0.0 and fetched["LIVE"]["metar_age_min"] == 35.0
    assert fetched["BACK"]["cur_precip_or_thunder"] == 1.0
    event = _rows(build_features(samples, metar, empty_f, empty_b, availability="event_time"))
    assert event["LIVE"]["cur_precip_or_thunder"] == 1.0
    with pytest.raises(ValueError):
        build_features(samples, metar, empty_f, empty_b, availability="bogus")


def test_forecast_backfilled_after_t_is_missing_not_filled(spark, frames):
    forecast = frames.forecast([("VVDN", "10:40", "11:00", 2.0, 0.0, 0.0, True)])
    feat = _rows(build_features(frames.samples([("VVDN", "10:30")]), frames.metar([]), forecast, frames.b13([])))
    assert feat["VVDN"]["forecast_missing"] == 1.0 and feat["VVDN"]["fc_rain_next_hour_mm"] is None


# --- cloud-top cooling ----------------------------------------------------------------------------

def test_cooling_delta_is_null_when_the_earlier_scan_is_missing(spark, frames):
    b13 = frames.b13([
        # VVDN: full 10-minute history -> both deltas
        ("VVDN", "10:00", -50.0), ("VVDN", "09:40", -35.0), ("VVDN", "09:30", -30.0),
        # VVNB: the scan 30' earlier is missing; the 15' one exists
        ("VVNB", "10:00", -50.0), ("VVNB", "09:40", -35.0),
        # VVTS: only a scan an hour earlier -> never interpolated across the gap
        ("VVTS", "10:00", -50.0), ("VVTS", "09:00", -10.0),
    ])
    feat = _rows(build_features(
        frames.samples([("VVDN", "10:05"), ("VVNB", "10:05"), ("VVTS", "10:05")]),
        frames.metar([]), frames.forecast([]), b13,
    ))
    assert feat["VVDN"]["bt_delta_15m_c"] == pytest.approx(-15.0)
    assert feat["VVDN"]["bt_delta_30m_c"] == pytest.approx(-20.0)
    assert feat["VVNB"]["bt_delta_15m_c"] == pytest.approx(-15.0)
    assert feat["VVNB"]["bt_delta_30m_c"] is None
    assert feat["VVTS"]["bt_delta_15m_c"] is None and feat["VVTS"]["bt_delta_30m_c"] is None
    assert feat["VVTS"]["inner_bt_min_c"] == -50.0 and feat["VVTS"]["b13_missing"] == 0.0


def test_latest_scan_older_than_60_minutes_counts_as_missing(spark, frames):
    feat = _rows(build_features(
        frames.samples([("VVDN", "10:00")]), frames.metar([]), frames.forecast([]),
        frames.b13([("VVDN", "08:50", -40.0)]),
    ))
    assert feat["VVDN"]["b13_missing"] == 1.0 and feat["VVDN"]["inner_bt_min_c"] is None


def test_prepare_model_frame_fills_nulls_and_flags_missing_deltas(spark, frames):
    b13 = frames.b13([("VVNB", "10:00", -50.0), ("VVNB", "09:40", -35.0)])
    feat = build_features(
        frames.samples([("VVNB", "10:05"), ("VVDN", "10:05")]), frames.metar([]), frames.forecast([]), b13
    )
    fill = fit_fill_values(feat)
    assert fill["inner_bt_min_c"] == pytest.approx(-50.0)
    prepared = {r["airport_icao"]: r.asDict() for r in prepare_model_frame(feat, fill).collect()}
    assert all(prepared["VVDN"][c] is not None for c in MODEL_FEATURES)
    assert prepared["VVDN"]["b13_missing"] == 1.0 and prepared["VVDN"]["inner_bt_min_c"] == pytest.approx(-50.0)
    assert prepared["VVNB"]["bt_delta_15m_missing"] == 0.0 and prepared["VVNB"]["bt_delta_30m_missing"] == 1.0
    assert prepared["VVNB"]["airport_VVNB"] == 1.0 and prepared["VVNB"]["airport_VVDN"] == 0.0


def test_sample_keys_stay_unique(spark, frames):
    metar = frames.metar([("VVDN", "10:00", False, 9999)] * 3)
    feat = build_features(frames.samples([("VVDN", "10:05")] * 2), metar, frames.forecast([]), frames.b13([]))
    assert feat.count() == 1 and feat.groupBy(*KEY).count().first()["count"] == 1


# --- baseline parity with the dashboard rule --------------------------------------------------------

def test_rule_baseline_matches_priority_rules(spark):
    grid = []
    for metar_present, age in [(False, None), (True, 0.0), (True, 60.0), (True, 61.0)]:
        for precip in (False, True):
            for low_vis in (False, True):
                for forecast_present, rain in [(False, None), (True, 0.0), (True, 0.05), (True, 0.1), (True, 2.0)]:
                    grid.append((metar_present, age, precip, low_vis, forecast_present, rain))
    rows = [
        (0.0 if present else 1.0, age, 1.0 if (precip or low_vis) and present else 0.0,
         0.0 if fpresent else 1.0, rain)
        for present, age, precip, low_vis, fpresent, rain in grid
    ]
    df = spark.createDataFrame(
        rows, "metar_missing double, metar_age_min double, cur_hazard double, forecast_missing double, fc_rain_next_hour_mm double"
    )
    got = [r["bl_rule"] for r in add_baselines(df).collect()]
    expected = []
    for present, age, precip, low_vis, fpresent, rain in grid:
        decision = evaluate_priority(PriorityInput(
            metar_is_present=present, metar_age_minutes=age,
            has_precip_or_thunder_at_station=precip and present, is_low_visibility=low_vis and present,
            station_weather_codes=[], visibility_m=None, observed_hhmm="08:00Z",
            forecast_is_present=fpresent, forecast_rain_next_hour_mm=rain, target_forecast_hhmm="09:00Z",
        ))
        expected.append(1 if decision.level in (1, 3) else 0)
    assert got == expected
