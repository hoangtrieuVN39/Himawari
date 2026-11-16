"""E7: split/gate/report logic (pure), Spark expression helpers, and a train -> infer round trip on a tiny lake."""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from src.ml.features import FEATURE_LABELS_VI, MODEL_FEATURES
from src.ml.infer import calibrated_probability, run_inference, top_features_column
from src.ml.train import (
    BASELINES, GAP_HOURS, TEST_DAYS, baseline_report, decide_gate, model_report, run_training, split_bounds,
)
from src.spark.lake_paths import local_table_version, new_run_id
from src.spark.writers import write_table_version


# --- pure logic --------------------------------------------------------------------------------------------

def test_split_bounds_keep_the_gap_and_a_seven_day_test_window():
    end = datetime(2026, 11, 16, 5, 30, tzinfo=timezone.utc)
    b = split_bounds(end)
    assert b["test_hi"] - b["test_lo"] == timedelta(days=TEST_DAYS)
    gap = timedelta(hours=GAP_HOURS)
    assert b["test_lo"] - b["val_hi"] >= gap and b["val_lo"] - b["train_hi"] >= gap
    assert b["train_hi"] < b["val_lo"] < b["val_hi"] < b["test_lo"] < b["test_hi"]


def test_gate_requires_beating_every_baseline_strictly():
    base = {"persistence": 0.70, "open_meteo_only": 0.47, "current_rule_l1_or_l3": 0.65}
    assert decide_gate(0.71, base)["gate_passed"] is True
    assert decide_gate(0.70, base)["gate_passed"] is False          # tie is not a win
    failed = decide_gate(0.68, base)
    assert failed["gate_passed"] is False and failed["best_baseline"] == "persistence"
    assert decide_gate(None, base)["gate_passed"] is False
    assert decide_gate(0.1, {"persistence": None, "open_meteo_only": None, "current_rule_l1_or_l3": None})["gate_passed"] is True


def _rows(n_pos=20, n_neg=60):
    rows = []
    for i in range(n_pos + n_neg):
        y = 1 if i < n_pos else 0
        rows.append({"y": y, "p_raw": 0.8 if y else 0.1, "airport_icao": "VVDN" if i % 2 else "VVNB",
                     "forecast_missing": i % 3 == 0 and 1 or 0, **{c: y for c in BASELINES.values()}})
    return rows


def test_model_report_calibrates_on_validation_and_scores_test_only():
    val, test = _rows(), _rows()
    report, platt = model_report(val, test)
    assert report["test"]["all"]["pr_auc"] == pytest.approx(1.0)
    assert report["test"]["all"]["csi"] == pytest.approx(1.0)
    assert 0.1 < report["threshold"] < 1.0
    assert len(report["reliability_test"]) == 10 and sum(b["n"] for b in report["reliability_test"]) == 80
    assert {"all", "airport:VVDN", "airport:VVNB", "window:live_forecast_available", "window:backfill_forecast_missing"} <= set(report["test"])
    assert report["test"]["all"]["brier"] < 0.05 and not math.isnan(platt["a"])


def test_baseline_report_has_no_probabilistic_metrics():
    rep = baseline_report(_rows())
    assert set(rep) == set(BASELINES)
    assert rep["persistence"]["all"]["csi"] == pytest.approx(1.0)
    assert "brier" not in rep["persistence"]["all"] and "pr_auc" not in rep["persistence"]["all"]


# --- Spark expression helpers --------------------------------------------------------------------------------

def test_top_features_picks_three_largest_signed_contributions(spark):
    names = MODEL_FEATURES[:4]
    weights = [2.0, -3.0, 0.5, 0.0]
    df = spark.createDataFrame([(Vectors.dense([1.0, 1.0, 1.0, 5.0]),)], ["features"])
    row = df.select(top_features_column(weights, names, F.col("features"), signed=True).alias("t")).first()
    assert list(row["t"]) == [FEATURE_LABELS_VI[names[1]] + " ↓", FEATURE_LABELS_VI[names[0]] + " ↑", FEATURE_LABELS_VI[names[2]] + " ↑"]
    # tree importances: unsigned, importance x |z|; zero-weight features are never listed
    row = df.select(top_features_column([0.0, 0.2, 0.1, 0.0], names, F.col("features"), signed=False).alias("t")).first()
    assert list(row["t"]) == [FEATURE_LABELS_VI[names[1]], FEATURE_LABELS_VI[names[2]]]


def test_calibrated_probability_matches_the_platt_formula(spark):
    df = spark.createDataFrame([(0.2,), (0.9,)], ["p"])
    out = [r["q"] for r in df.select(calibrated_probability(F.col("p"), {"a": 1.0, "b": 0.0}).alias("q")).collect()]
    assert out == pytest.approx([0.2, 0.9])
    shifted = df.select(calibrated_probability(F.col("p"), {"a": 2.0, "b": 1.0}).alias("q")).collect()
    expected = [1 / (1 + math.exp(-(2 * math.log(p / (1 - p)) + 1))) for p in (0.2, 0.9)]
    assert [r["q"] for r in shifted] == pytest.approx(expected)


# --- train -> infer on a tiny synthetic lake ---------------------------------------------------------------------

def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc)


def test_train_then_infer_round_trip(spark, tmp_path: Path):
    lake = str(tmp_path / "lake")
    start = datetime(2026, 10, 1, 0, 0)
    metar_rows = []
    for icao in ("VVDN", "VVNB"):
        for k in range(18 * 48):  # 18 days of half-hourly reports; wet spells 03:00-07:30 UTC every day
            t = start + timedelta(minutes=30 * k)
            wet = 3 <= t.hour < 8
            metar_rows.append((icao, _utc(t), _utc(t + timedelta(minutes=5)), True, 1, 3000 if wet else 9999, wet, False))
    metar = spark.createDataFrame(metar_rows, StructType([
        StructField("airport_icao", StringType()), StructField("observed_at_utc", TimestampType()),
        StructField("fetched_at_utc", TimestampType()), StructField("is_backfill", BooleanType()),
        StructField("revision_index", IntegerType()), StructField("visibility_m", IntegerType()),
        StructField("has_precip_or_thunder_at_station", BooleanType()), StructField("has_vicinity_weather", BooleanType()),
    ]))
    forecast = spark.createDataFrame(
        [("VVDN", _utc(start), _utc(start + timedelta(hours=1)), 0.0, 0.0, 0.0, False)],
        "airport_icao string, fetched_at_utc timestamp, forecast_valid_at_utc timestamp, precipitation_mm double, "
        "rain_mm double, showers_mm double, is_backfill boolean",
    )
    b13 = spark.createDataFrame(
        [("VVDN", _utc(start), _utc(start), False, "AVAILABLE", False, -10.0, 0.0, 0.0, 1.0)],
        "airport_icao string, scan_time_utc timestamp, fetched_at_utc timestamp, is_backfill boolean, status string, "
        "is_synthetic_test boolean, inner_bt_min_c double, inner_cold_cloud_fraction double, "
        "inner_severe_convection_fraction double, b13_valid_fraction double",
    )
    last = start + timedelta(days=17, hours=20)
    decision = spark.createDataFrame(
        [(icao, _utc(last), 4) for icao in ("VVDN", "VVNB")] + [(icao, _utc(last - timedelta(hours=8)), 1) for icao in ("VVDN", "VVNB")],
        "airport_icao string, review_slot_utc timestamp, priority_level int",
    )
    for name, df in (("metar_observations", metar), ("open_meteo_forecasts", forecast), ("himawari_b13_metrics", b13), ("decision_table", decision)):
        write_table_version(df, lake, name, new_run_id())

    models_dir = str(tmp_path / "models")
    meta = run_training(spark, lake, models_dir)
    assert (Path(models_dir) / "LATEST").read_text(encoding="utf-8") == meta["run_id"]
    on_disk = json.loads((Path(models_dir) / meta["run_id"] / "metadata.json").read_text(encoding="utf-8"))
    assert on_disk["features"] == MODEL_FEATURES and isinstance(on_disk["gate_passed"], bool)
    assert set(on_disk["baselines"]) == set(BASELINES) and set(on_disk["metrics"]) == {"lr", "gbt"}
    bounds = on_disk["split"]["bounds_utc"]
    assert bounds["train_hi"] < bounds["val_lo"] < bounds["val_hi"] < bounds["test_lo"] < bounds["test_hi"]
    for name in ("scaler", "lr", "gbt"):
        assert (Path(models_dir) / meta["run_id"] / name).is_dir()

    summary = run_inference(spark, lake, models_dir)
    assert summary["rows"] == 4 and summary["model_run_id"] == meta["run_id"]
    version = local_table_version(lake, "nowcast")
    assert version is not None
    rows = spark.read.parquet(str(version)).collect()
    assert len(rows) == 4
    for r in rows:
        assert 0.0 <= r["probability"] <= 1.0 and r["gate_passed"] == on_disk["gate_passed"]
        assert r["model_run_id"] == meta["run_id"] and len(r["top_features"]) <= 3
        assert r["predicted_positive"] == (r["probability"] >= r["threshold"])
