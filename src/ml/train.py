"""E7 training and evaluation of the 0-60 minute nowcast.

CLI: ``python -m src.ml.train --storage-root data [--lake-root ...] [--models-dir models]``

Reads Gold `feature_table` (built by `python -m src.ml.features`; built here when missing) and trains,
with Spark MLlib, a standardised LogisticRegression and a GBTClassifier on rows with a known label.

Split (chronological, walk-forward style; every pair of sets is separated by a gap of >= 2 h, longer than
the 60' label horizon, so no label window straddles two sets)::

    train  |gap 2h|  validation (5 days)  |gap 2h|  test (last 7 days, held out)

Validation is used for: Platt calibration of the probabilities, the operating threshold (max CSI of
`probability >= threshold`) and the choice of the better model. The test window is only scored.

Baselines are deterministic and scored on the same test rows: persistence (current METAR already meets
the Level-1 condition), Open-Meteo-only (next-hour rain >= 0.1 mm) and the dashboard rule at `t`
(Level 1 or 3). PR-AUC, Brier score and the 10-bin reliability table are reported for the models only;
CSI/POD/FAR are reported for models (at the validation threshold) and baselines.

Gate: ``gate_passed = CSI_test(selected model) > max(CSI_test of the three baselines)``. The dashboard
shows the nowcast only when the gate passes.

Artifacts: ``models/<run_id>/{scaler,lr,gbt}`` (Spark ML models), ``models/<run_id>/metadata.json`` and
the pointer file ``models/LATEST``.

Known limitation (also in metadata): Open-Meteo forecasts exist only from the day the collector started,
so the training window usually has `forecast_missing = 1` for every row; the models then cannot learn the
forecast features and the live-window evaluation measures them out of sample.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ml import evaluate as ev
from src.ml.features import (
    FEATURE_LABELS_VI,
    MODEL_FEATURES,
    fit_fill_values,
    prepare_model_frame,
    run_feature_table,
)
from src.spark.lake_paths import default_lake_root, new_run_id
from src.spark.writers import read_table

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODELS_DIR = "models"
GAP_HOURS = 2
VAL_DAYS = 5
TEST_DAYS = 7
BASELINES = {
    "persistence": "bl_persistence",
    "open_meteo_only": "bl_open_meteo",
    "current_rule_l1_or_l3": "bl_rule",
}
_TS = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def split_bounds(t_end: datetime) -> dict[str, datetime]:
    """Chronological cut points (see module docstring); all sets are half-open (lo, hi]."""
    gap = timedelta(hours=GAP_HOURS)
    test_lo = t_end - timedelta(days=TEST_DAYS)
    val_hi = test_lo - gap
    val_lo = val_hi - timedelta(days=VAL_DAYS)
    train_hi = val_lo - gap
    return {"train_hi": train_hi, "val_lo": val_lo, "val_hi": val_hi, "test_lo": test_lo, "test_hi": t_end}


def _in_range(lo: datetime | None, hi: datetime) -> Any:
    t = F.col("sample_time_utc")
    cond = t <= F.to_timestamp(F.lit(_iso(hi)))
    return cond if lo is None else cond & (t > F.to_timestamp(F.lit(_iso(lo))))


def _git_info() -> dict[str, Any]:
    def run(*args: str) -> str | None:
        try:
            out = subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, timeout=20)
            return out.stdout.strip() if out.returncode == 0 else None
        except Exception:
            return None

    return {"git_sha": run("rev-parse", "HEAD"), "git_dirty": bool(run("status", "--porcelain", "--", "src", "tests"))}


def _split_summary(df: DataFrame) -> dict[str, Any]:
    r = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("y")).alias("positives"),
        F.sum(1 - F.col("forecast_missing").cast("int")).alias("rows_with_forecast"),
        F.sum(1 - F.col("b13_missing").cast("int")).alias("rows_with_b13"),
        F.date_format(F.min("sample_time_utc"), _TS).alias("first_t"),
        F.date_format(F.max("sample_time_utc"), _TS).alias("last_t"),
    ).first()
    out = r.asDict()
    out["positives"] = int(out["positives"] or 0)
    out["positive_rate"] = out["positives"] / out["rows"] if out["rows"] else None
    return out


def _collect(scored: DataFrame) -> list[dict[str, Any]]:
    cols = [
        F.col("y").cast("int").alias("y"),
        F.col("p_raw"),
        "airport_icao",
        F.col("forecast_missing").cast("int").alias("forecast_missing"),
        *[F.col(c).cast("int").alias(c) for c in BASELINES.values()],
    ]
    return [r.asDict() for r in scored.select(*cols).collect()]


def _subsets(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out = {"all": rows}
    for airport in sorted({r["airport_icao"] for r in rows}):
        out[f"airport:{airport}"] = [r for r in rows if r["airport_icao"] == airport]
    out["window:live_forecast_available"] = [r for r in rows if r["forecast_missing"] == 0]
    out["window:backfill_forecast_missing"] = [r for r in rows if r["forecast_missing"] == 1]
    return out


def _f4(value: Any) -> str:
    return f"{value:.4f}" if isinstance(value, float) else str(value)


def baseline_report(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Deterministic baselines: 2x2 counts and CSI/POD/FAR per subset."""
    out: dict[str, dict[str, Any]] = {}
    for name, col in BASELINES.items():
        out[name] = {}
        for subset, part in _subsets(rows).items():
            if not part:
                out[name][subset] = {"n": 0}
                continue
            out[name][subset] = ev.binary_scores([r["y"] for r in part], [r[col] for r in part])
    return out


def model_report(
    val_rows: list[dict[str, Any]], test_rows: list[dict[str, Any]]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Platt calibration + operating point on validation; metrics on the held-out test rows."""
    y_val = [r["y"] for r in val_rows]
    platt = ev.fit_platt([r["p_raw"] for r in val_rows], y_val)
    for r in val_rows:
        r["p"] = ev.apply_platt([r["p_raw"]], platt)[0]
    for r in test_rows:
        r["p"] = ev.apply_platt([r["p_raw"]], platt)[0]
    point = ev.best_csi_threshold(y_val, [r["p"] for r in val_rows])
    thr = point["threshold"]

    def block(part: list[dict[str, Any]]) -> dict[str, Any]:
        if not part:
            return {"n": 0}
        y = [r["y"] for r in part]
        p = [r["p"] for r in part]
        scores = ev.binary_scores(y, [1 if v >= thr else 0 for v in p])
        return {**scores, "pr_auc": ev.pr_auc(y, p), "brier": ev.brier_score(y, p), "prevalence": sum(y) / len(y)}

    test_blocks = {name: block(part) for name, part in _subsets(test_rows).items()}
    test_all_y = [r["y"] for r in test_rows]
    test_all_p = [r["p"] for r in test_rows]
    report = {
        "calibration": {"method": "platt", **platt},
        "threshold": thr,
        "threshold_fallback": point["fallback"],
        "validation_csi_at_threshold": point["csi"],
        "validation": block(val_rows),
        "test": test_blocks,
        "reliability_test": ev.reliability_bins(test_all_y, test_all_p, 10),
    }
    return report, platt


def decide_gate(model_test_csi: float | None, baseline_test_csi: dict[str, float | None]) -> dict[str, Any]:
    """Strict gate: the model must beat every baseline. An undefined CSI (no hit at all) counts as 0."""
    scored = {name: (v if v is not None else 0.0) for name, v in baseline_test_csi.items()}
    best_name = max(scored, key=scored.get)
    passed = model_test_csi is not None and model_test_csi > scored[best_name]
    return {
        "gate_passed": bool(passed),
        "model_csi_test": model_test_csi,
        "best_baseline": best_name,
        "best_baseline_csi_test": scored[best_name],
        "baseline_csi_test": scored,
        "rule": "gate_passed = CSI_test(selected model) > max(CSI_test of persistence, open_meteo_only, current_rule_l1_or_l3)",
    }


def _scale(df: DataFrame, assembler: VectorAssembler, scaler: Any) -> DataFrame:
    return scaler.transform(assembler.transform(df))


def run_training(spark: SparkSession, lake_root: str, models_dir: str = DEFAULT_MODELS_DIR) -> dict[str, Any]:
    table = read_table(spark, lake_root, "feature_table")
    if table is None:
        run_feature_table(spark, lake_root)
        table = read_table(spark, lake_root, "feature_table")
    labelled = table.filter(F.col("label_known")).cache()
    t_end_text = labelled.agg(F.date_format(F.max("sample_time_utc"), _TS)).first()[0]
    if t_end_text is None:
        raise SystemExit("feature_table has no row with a known label")
    bounds = split_bounds(_parse(t_end_text))

    train_raw = labelled.filter(_in_range(None, bounds["train_hi"]))
    fill = fit_fill_values(train_raw)
    prepared = prepare_model_frame(labelled, fill)
    splits = {
        "train": prepared.filter(_in_range(None, bounds["train_hi"])),
        "validation": prepared.filter(_in_range(bounds["val_lo"], bounds["val_hi"])),
        "test": prepared.filter(_in_range(bounds["test_lo"], bounds["test_hi"])),
    }
    summaries = {name: _split_summary(df) for name, df in splits.items()}
    for name, summary in summaries.items():
        if summary["rows"] == 0:
            raise SystemExit(f"split '{name}' is empty: {summaries}")
    if summaries["train"]["positives"] in (0, summaries["train"]["rows"]):
        raise SystemExit(f"training split has a single class: {summaries['train']}")

    assembler = VectorAssembler(inputCols=MODEL_FEATURES, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True).fit(
        assembler.transform(splits["train"])
    )
    train_s = _scale(splits["train"], assembler, scaler).withColumn("label", F.col("y").cast("double")).cache()
    val_s = _scale(splits["validation"], assembler, scaler).withColumn("label", F.col("y").cast("double"))
    test_s = _scale(splits["test"], assembler, scaler).withColumn("label", F.col("y").cast("double"))

    lr = LogisticRegression(
        featuresCol="features", labelCol="label", regParam=0.01, elasticNetParam=0.0, maxIter=200, standardization=False
    ).fit(train_s)
    gbt = GBTClassifier(
        featuresCol="features", labelCol="label", maxIter=40, maxDepth=3, stepSize=0.1,
        subsamplingRate=0.8, minInstancesPerNode=5, seed=42,
    ).fit(train_s)
    models = {"lr": lr, "gbt": gbt}

    def scored(model: Any, df: DataFrame) -> list[dict[str, Any]]:
        return _collect(model.transform(df).withColumn("p_raw", vector_to_array("probability")[1]))

    test_rows_any = None
    reports: dict[str, Any] = {}
    for name, model in models.items():
        val_rows = scored(model, val_s)
        test_rows = scored(model, test_s)
        test_rows_any = test_rows
        reports[name], _ = model_report(val_rows, test_rows)

    baselines = baseline_report(test_rows_any)
    baseline_csi = {k: v["all"].get("csi") for k, v in baselines.items()}

    def val_csi(name: str) -> float:
        return reports[name]["validation_csi_at_threshold"] or 0.0

    selected = "gbt" if val_csi("gbt") > val_csi("lr") else "lr"
    gate = decide_gate(reports[selected]["test"]["all"].get("csi"), baseline_csi)

    run_id = new_run_id()
    out_dir = Path(models_dir) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    scaler.write().overwrite().save(str(out_dir / "scaler"))
    lr.write().overwrite().save(str(out_dir / "lr"))
    gbt.write().overwrite().save(str(out_dir / "gbt"))

    metadata = {
        "run_id": run_id,
        "created_at_utc": _iso(datetime.now(timezone.utc)),
        **_git_info(),
        "lake_root": lake_root,
        "target": "any METAR in (t, t+60'] with precip/thunder at station or visibility < 5000 m",
        "split": {
            "method": f"chronological holdout, {GAP_HOURS} h gap between sets, last {TEST_DAYS} days = test",
            "bounds_utc": {k: _iso(v) for k, v in bounds.items()},
            "sets": summaries,
        },
        "features": MODEL_FEATURES,
        "feature_labels_vi": {f: FEATURE_LABELS_VI[f] for f in MODEL_FEATURES},
        "fill_values": fill,
        "scaler": {"mean": [float(x) for x in scaler.mean], "std": [float(x) for x in scaler.std]},
        "models": {
            "lr": {"regParam": 0.01, "elasticNetParam": 0.0, "maxIter": 200, "intercept": float(lr.intercept),
                   "coefficients": dict(zip(MODEL_FEATURES, [float(c) for c in lr.coefficients]))},
            "gbt": {"maxIter": 40, "maxDepth": 3, "stepSize": 0.1, "seed": 42,
                    "feature_importances": dict(zip(MODEL_FEATURES, [float(c) for c in gbt.featureImportances]))},
        },
        "metrics": reports,
        "baselines": baselines,
        "selected_model": selected,
        "selection_rule": "higher validation CSI at its own validation threshold (ties -> lr)",
        "threshold": reports[selected]["threshold"],
        "calibration": reports[selected]["calibration"],
        "gate": gate,
        "gate_passed": gate["gate_passed"],
        "explanation_method": "lr_coef_x_standardized_value" if selected == "lr" else "gbt_importance_x_abs_standardized_value",
        "limitations": [
            "Open-Meteo forecasts exist only since collection started: forecast_missing=1 for the whole training window "
            "when the live window is shorter than the held-out week, so forecast features cannot be learned.",
            "Overlapping 60-minute labels at 30-minute sampling make rows strongly autocorrelated; CIs are not reported for model metrics.",
            "B13 coverage is sparse; b13_missing=1 for most rows.",
        ],
    }
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    (Path(models_dir) / "LATEST").write_text(run_id, encoding="utf-8")
    labelled.unpersist()
    train_s.unpersist()
    return metadata


def format_report(meta: dict[str, Any]) -> str:
    lines = [f"run_id {meta['run_id']}  git {meta.get('git_sha')}  selected={meta['selected_model']}  threshold={meta['threshold']}"]
    for name, s in meta["split"]["sets"].items():
        lines.append(f"  {name:<10} rows={s['rows']:<5} pos={s['positives']:<4} fc={s['rows_with_forecast']:<5} b13={s['rows_with_b13']:<4} {s['first_t']} .. {s['last_t']}")
    lines.append("test (all airports): method | n | pos | CSI | POD | FAR | PR-AUC | Brier")
    for name in ("lr", "gbt"):
        t = meta["metrics"][name]["test"]["all"]
        lines.append(f"  {name:<22} {t['n']:<5} {t['n_events']:<4} {_f4(t['csi'])} {_f4(t['pod'])} {_f4(t['far'])} {_f4(t['pr_auc'])} {_f4(t['brier'])}")
    for name, v in meta["baselines"].items():
        t = v["all"]
        lines.append(f"  {name:<22} {t['n']:<5} {t['n_events']:<4} {_f4(t['csi'])} {_f4(t['pod'])} {_f4(t['far'])} - -")
    lines.append(f"gate: {json.dumps(meta['gate'], ensure_ascii=False)}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Train and evaluate the 0-60' nowcast (Spark MLlib)")
    parser.add_argument("--storage-root", default="data")
    parser.add_argument("--lake-root", default=None)
    parser.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    args = parser.parse_args(argv)
    from src.spark.session import get_spark_session

    spark = get_spark_session("AirportNowcastTrain")
    lake = args.lake_root or default_lake_root(args.storage_root)
    print(format_report(run_training(spark, lake, args.models_dir)))


if __name__ == "__main__":
    main()
