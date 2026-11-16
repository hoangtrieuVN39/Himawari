"""E5: forecast verification contract (issuance cut-off, window boundaries, back-fill exclusion) and level order."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.analytics.verification import (
    SCHEMA, forecast_counts, forecast_units, level_counts, level_rows, level_units, literal_frame,
    skill_rows,
)
from src.ml.evaluate import wilson_interval


def _skill(spark, frames, forecast_rows, metar_rows):
    counts = forecast_counts(forecast_units(frames.forecast(forecast_rows), frames.metar(metar_rows)))
    rows = skill_rows(counts)
    return {(r["airport_icao"], r["threshold_mm"]): r for r in rows if r["section"] == "forecast_skill"}


def test_forecast_fetched_after_issuance_is_excluded(spark, frames):
    # T = 11:00 -> issuance 10:00. A: the only usable version says 0 mm, the 10:30 one (2 mm) is too late.
    # B: a version fetched exactly at issuance is usable.
    forecast = [
        ("A", "09:00", "11:00", 0.0, 0.0, 0.0), ("A", "10:30", "11:00", 2.0, 0.0, 0.0),
        ("B", "10:00", "11:00", 2.0, 0.0, 0.0),
    ]
    metar = [(i, h, p, 9999) for i in ("A", "B") for h, p in (("10:30", True), ("11:00", False))]
    out = _skill(spark, frames, forecast, metar)
    a, b = out[("A", 0.1)], out[("B", 0.1)]
    assert (a["hits"], a["misses"], a["false_alarms"]) == (0, 1, 0)
    assert (b["hits"], b["misses"]) == (1, 0)


def test_metar_at_t_minus_1h_is_not_counted_and_at_t_is(spark, frames):
    forecast = [("C", "08:00", "11:00", 1.0, 0.0, 0.0), ("D", "08:00", "11:00", 1.0, 0.0, 0.0)]
    metar = [
        ("C", "10:00", True, 9999), ("C", "10:30", False, 9999), ("C", "11:00", False, 9999),   # hazard only at T-1h
        ("D", "10:00", False, 9999), ("D", "10:30", False, 9999), ("D", "11:00", True, 9999),   # hazard at T
    ]
    out = _skill(spark, frames, forecast, metar)
    assert (out[("C", 0.1)]["false_alarms"], out[("C", 0.1)]["hits"]) == (1, 0)   # event NOT observed
    assert (out[("D", 0.1)]["hits"], out[("D", 0.1)]["false_alarms"]) == (1, 0)   # event observed


def test_low_visibility_alone_is_not_an_observed_event(spark, frames):
    forecast = [("V", "08:00", "11:00", 1.0, 0.0, 0.0)]
    metar = [("V", "10:30", False, 3000), ("V", "11:00", False, 9999)]
    assert _skill(spark, frames, forecast, metar)[("V", 0.1)]["false_alarms"] == 1


def test_backfilled_forecasts_are_excluded(spark, frames):
    forecast = [
        ("E", "08:00", "11:00", 5.0, 0.0, 0.0, True),                                   # only a back-fill -> no unit
        ("F", "07:00", "11:00", 0.0, 0.0, 0.0), ("F", "09:00", "11:00", 5.0, 0.0, 0.0, True),
    ]
    metar = [(i, h, p, 9999) for i in ("E", "F") for h, p in (("10:30", True), ("11:00", False))]
    out = _skill(spark, frames, forecast, metar)
    assert ("E", 0.1) not in out
    assert (out[("F", 0.1)]["misses"], out[("F", 0.1)]["hits"]) == (1, 0)


def test_units_without_judgeable_observations_are_counted_not_scored(spark, frames):
    forecast = [("G", "08:00", "11:00", 1.0, 0.0, 0.0), ("H", "08:00", "11:00", 1.0, 0.0, 0.0),
                ("I", "08:00", "11:00", 1.0, 0.0, 0.0)]
    metar = [
        ("G", "09:30", False, 9999), ("G", "11:30", False, 9999),    # newest METAR after T but none inside (10:00, 11:00]
        ("H", "10:30", True, 9999),                                  # newest METAR (10:30) is before T: window incomplete
        ("I", "10:30", False, 9999), ("I", "11:00", False, 9999),
    ]
    out = _skill(spark, frames, forecast, metar)
    assert out[("G", 0.1)]["n"] == 0 and out[("G", 0.1)]["n_excluded_no_metar"] == 1
    assert ("H", 0.1) not in out or out[("H", 0.1)]["n"] == 0
    assert out[("I", 0.1)]["n"] == 1


def test_threshold_sensitivity_and_rain_formula(spark, frames):
    # 0.3 mm via rain + showers (precipitation 0): event at 0.1, no event at 0.5 / 1.0
    forecast = [("T1", "08:00", "11:00", 0.0, 0.2, 0.1)]
    metar = [("T1", "10:30", True, 9999), ("T1", "11:00", False, 9999)]
    out = _skill(spark, frames, forecast, metar)
    assert out[("T1", 0.1)]["hits"] == 1
    assert out[("T1", 0.5)]["misses"] == 1 and out[("T1", 1.0)]["misses"] == 1
    assert out[("T1", 0.1)]["pod"] == 1.0 and out[("T1", 0.5)]["pod"] == 0.0
    assert out[("T1", 0.5)]["far"] is None            # no forecast event -> FAR undefined, not 0
    pooled = out[("ALL", 0.1)]
    assert pooled["n"] == 1 and pooled["event_rate"] == 1.0 and pooled["forecast_age_median_h"] == pytest.approx(2.0)


def _decision(spark, rows):
    return spark.createDataFrame(
        [(i, datetime(2026, 11, 10, int(h[:2]), int(h[3:]), tzinfo=timezone.utc), lv) for i, h, lv in rows],
        "airport_icao string, review_slot_utc timestamp, priority_level int",
    )


def test_level_event_rates_use_the_60_minute_window_after_the_slot(spark, frames):
    metar = frames.metar([
        ("X", "10:00", True, 9999), ("X", "10:30", False, 9999), ("X", "11:00", True, 9999),
        ("Y", "10:00", False, 9999), ("Y", "10:30", False, 9999), ("Y", "11:00", False, 9999),
    ])
    decision = _decision(spark, [
        ("X", "10:00", 1),   # window (10:00, 11:00] contains the 11:00 hazard -> event
        ("Y", "10:00", 4),   # no event
        ("Y", "10:30", 3),   # window reaches 11:30 > newest METAR 11:00 -> not evaluable, dropped
    ])
    counts = level_counts(level_units(decision, metar))
    rows = {(r["section"], r["airport_icao"], r["level"]): r
            for r in level_rows(counts) if r["section"] == "level_event_rate"}
    assert rows[("level_event_rate", "ALL", 1)]["n_events"] == 1 and rows[("level_event_rate", "ALL", 1)]["n"] == 1
    assert rows[("level_event_rate", "ALL", 4)]["event_rate"] == 0.0
    assert rows[("level_event_rate", "ALL", 3)]["n"] == 0 and rows[("level_event_rate", "ALL", 3)]["event_rate"] is None
    lo, hi = wilson_interval(1, 1)
    assert rows[("level_event_rate", "ALL", 1)]["ci_low"] == pytest.approx(lo)


def test_monotonic_check(spark):
    def counts(rates):
        return [
            {"airport_icao": "VVDN", "priority_level": lv, "n": 100, "precip_events": int(r * 100), "hazard_events": int(r * 100),
             "n_excluded_no_metar": 0, "window_start": "2026-11-10T00:00:00Z", "window_end": "2026-11-10T01:00:00Z"}
            for lv, r in rates.items()
        ]

    def check(rates):
        rows = level_rows(counts(rates))
        return next(r for r in rows if r["section"] == "level_monotonic" and r["airport_icao"] == "ALL")

    assert check({1: 0.5, 2: 0.1, 3: 0.3, 4: 0.1})["monotonic_ok"] is True
    assert check({1: 0.2, 2: 0.9, 3: 0.3, 4: 0.1})["monotonic_ok"] is False   # L1 < L3; L2 is ignored
    assert check({1: 0.5, 3: 0.3})["monotonic_ok"] is None                     # level 4 missing -> not evaluable


def test_literal_frame_matches_schema_and_handles_nulls(spark, frames):
    forecast = [("A", "08:00", "11:00", 1.0, 0.0, 0.0)]
    metar = [("A", "10:30", True, 9999), ("A", "11:00", False, 9999)]
    counts = forecast_counts(forecast_units(frames.forecast(forecast), frames.metar(metar)))
    df = literal_frame(spark, skill_rows(counts), SCHEMA, "20261116T000000000000Z")
    assert [f.name for f in df.schema.fields] == [n for n, _ in SCHEMA]
    row = df.filter("airport_icao = 'A' and threshold_mm = 0.1").first()
    assert row["hits"] == 1 and row["level"] is None and row["run_id"] == "20261116T000000000000Z"
    assert df.schema["window_start_utc"].dataType.simpleString() == "timestamp"
