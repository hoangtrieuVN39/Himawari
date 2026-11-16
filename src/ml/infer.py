"""E7 inference: score every Gold decision slot with the latest trained model -> Gold `nowcast`.

CLI: ``python -m src.ml.infer --storage-root data [--lake-root ...] [--models-dir models]``

For each (review_slot_utc, airport_icao) of the current `decision_table` the features are rebuilt at
`t = review_slot` with the same code as training (`build_features`, METAR/B13 availability 'fetched':
a report fetched after the slot is not visible unless it is a back-fill, exactly like the replay join),
standardised with the saved scaler, scored, calibrated with the saved Platt parameters and compared to the
saved operating threshold.

`top_features` holds the 3 largest contributions as Vietnamese strings: for the logistic regression
|coefficient x standardised value| with an arrow for the direction (↑ raises the risk, ↓ lowers it); for a
GBT, importance x |standardised value| without direction.

`gate_passed` is copied from the model metadata. The dashboard hides the column when it is false. The
nowcast is informative only and never changes the Level 1-4 order.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from pyspark.ml.classification import GBTClassificationModel, LogisticRegressionModel
from pyspark.ml.feature import StandardScalerModel, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ml.evaluate import PROB_EPS
from src.ml.features import FEATURE_LABELS_VI, build_features, prepare_model_frame
from src.ml.train import DEFAULT_MODELS_DIR
from src.spark.lake_paths import default_lake_root, new_run_id
from src.spark.writers import read_table, write_table_version

TOP_K = 3


def load_run(models_dir: str) -> tuple[Path, dict[str, Any]]:
    pointer = Path(models_dir) / "LATEST"
    if not pointer.exists():
        raise SystemExit(f"No trained model: {pointer} is missing. Run `python -m src.ml.train` first.")
    run_dir = Path(models_dir) / pointer.read_text(encoding="utf-8").strip()
    return run_dir, json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))


def calibrated_probability(p_raw: Column, platt: dict[str, Any]) -> Column:
    """sigmoid(a * logit(p) + b), the Platt map fitted on validation."""
    p = F.greatest(F.least(p_raw, F.lit(1.0 - PROB_EPS)), F.lit(PROB_EPS))
    z = F.lit(float(platt["a"])) * F.log(p / (F.lit(1.0) - p)) + F.lit(float(platt["b"]))
    return F.lit(1.0) / (F.lit(1.0) + F.exp(-z))


def top_features_column(weights: Sequence[float], features: Sequence[str], scaled: Column, signed: bool) -> Column:
    """Array of the TOP_K most influential feature labels (Spark higher-order functions, no Python UDF)."""
    contrib = F.zip_with(F.array(*[F.lit(float(w)) for w in weights]), vector_to_array(scaled), lambda w, z: w * z if signed else w * F.abs(z))
    labelled = F.zip_with(
        contrib,
        F.array(*[F.lit(FEATURE_LABELS_VI[f]) for f in features]),
        lambda c, label: F.struct(F.abs(c).alias("a"), c.alias("c"), label.alias("label")),
    )
    ranked = F.array_sort(
        F.filter(labelled, lambda x: x["a"] > F.lit(1e-12)),
        lambda x, y: F.when(x["a"] > y["a"], F.lit(-1)).when(x["a"] < y["a"], F.lit(1)).otherwise(F.lit(0)),
    )
    if signed:
        text = lambda x: F.concat(x["label"], F.when(x["c"] > 0, F.lit(" ↑")).otherwise(F.lit(" ↓")))
    else:
        text = lambda x: x["label"]
    return F.transform(F.slice(ranked, 1, TOP_K), text)


def score_samples(
    samples: DataFrame, metar: DataFrame, forecast: DataFrame, b13: DataFrame, run_dir: Path, meta: dict[str, Any]
) -> DataFrame:
    """Probability, threshold decision and top features for each (airport_icao, sample_time_utc) row."""
    feats = build_features(samples, metar, forecast, b13, availability="fetched")
    prepared = prepare_model_frame(feats, meta["fill_values"])
    assembler = VectorAssembler(inputCols=meta["features"], outputCol="features_raw")
    scaler = StandardScalerModel.load(str(run_dir / "scaler"))
    name = meta["selected_model"]
    if name == "lr":
        model = LogisticRegressionModel.load(str(run_dir / "lr"))
        weights, signed = model.coefficients.toArray(), True
    else:
        model = GBTClassificationModel.load(str(run_dir / "gbt"))
        weights, signed = model.featureImportances.toArray(), False
    scored = model.transform(scaler.transform(assembler.transform(prepared)))
    threshold = float(meta["threshold"])
    return scored.select(
        "airport_icao",
        "sample_time_utc",
        calibrated_probability(vector_to_array("probability")[1], meta["calibration"]).alias("probability"),
        top_features_column(weights, meta["features"], F.col("features"), signed).alias("top_features"),
    ).withColumn("threshold", F.lit(threshold)).withColumn(
        "predicted_positive", F.col("probability") >= F.lit(threshold)
    )


def run_inference(spark: SparkSession, lake_root: str, models_dir: str = DEFAULT_MODELS_DIR) -> dict[str, Any]:
    run_dir, meta = load_run(models_dir)
    tables = {n: read_table(spark, lake_root, n) for n in ("decision_table", "metar_observations", "open_meteo_forecasts", "himawari_b13_metrics")}
    missing = [n for n, d in tables.items() if d is None]
    if missing:
        raise SystemExit(f"Lake tables without a committed version: {missing}. Run src.spark.run_pipeline first.")
    samples = tables["decision_table"].select(
        "airport_icao", F.col("review_slot_utc").alias("sample_time_utc")
    ).dropDuplicates()
    scored = score_samples(samples, tables["metar_observations"], tables["open_meteo_forecasts"], tables["himawari_b13_metrics"], run_dir, meta)
    out = (
        scored.withColumnRenamed("sample_time_utc", "review_slot_utc")
        .withColumn("model_run_id", F.lit(meta["run_id"]))
        .withColumn("model_name", F.lit(meta["selected_model"]))
        .withColumn("gate_passed", F.lit(bool(meta["gate_passed"])))
        .withColumn("scored_at_utc", F.current_timestamp())
        .withColumn("review_date", F.to_date("review_slot_utc"))
        .select(
            "review_slot_utc", "airport_icao", "probability", "threshold", "predicted_positive",
            "model_run_id", "model_name", "gate_passed", "top_features", "scored_at_utc", "review_date",
        )
        .cache()
    )
    run_id = new_run_id()
    write_table_version(out, lake_root, "nowcast", run_id, ["review_date"])
    summary = out.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("predicted_positive").cast("int")).alias("predicted_positive"),
        F.avg("probability").alias("mean_probability"),
        F.max("probability").alias("max_probability"),
    ).first().asDict()
    out.unpersist()
    return {"run_id": run_id, "model_run_id": meta["run_id"], "gate_passed": meta["gate_passed"], "model": meta["selected_model"], **summary}


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Score Gold decision slots with the latest 0-60' nowcast model")
    parser.add_argument("--storage-root", default="data")
    parser.add_argument("--lake-root", default=None)
    parser.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    args = parser.parse_args(argv)
    from src.spark.session import get_spark_session

    spark = get_spark_session("AirportNowcastInfer")
    lake = args.lake_root or default_lake_root(args.storage_root)
    print(json.dumps(run_inference(spark, lake, args.models_dir), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
