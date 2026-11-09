"""End-to-end PySpark ETL pipeline for airport weather review."""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from src.spark.priority_engine import apply_priority_rules
from src.spark.quarantine import (
    build_reconciliation_report,
    inject_synthetic_quarantine_batch,
    validate_and_split_himawari,
    validate_and_split_metar,
    validate_and_split_open_meteo,
)
from src.spark.readers import (
    read_raw_himawari_metrics,
    read_raw_metar,
    read_raw_open_meteo,
)
from src.spark.session import get_spark_session
from src.spark.temporal_join import build_point_in_time_snapshot
from src.spark.transforms import enrich_valid_himawari, enrich_valid_metar


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parses CLI arguments for PySpark processing pipeline."""
    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline for Airport Weather Review")
    parser.add_argument(
        "--storage-root",
        type=str,
        default="data",
        help="Root storage directory or HDFS URI (default: data, or hdfs://namenode:9000/data)",
    )
    parser.add_argument(
        "--review-slots",
        type=str,
        default="2026-11-09T07:00:00Z,2026-11-09T08:00:00Z",
        help="Comma-separated ISO UTC review slot timestamps (default: 07:00Z,08:00Z replay slots)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["live", "replay"],
        default="replay",
        help="Pipeline mode: 'live' strictly enforces fetched_at <= review_slot; 'replay' supports backfilled history",
    )
    parser.add_argument(
        "--inject-quarantine-test",
        action="store_true",
        help="Inject synthetic invalid test records to verify quarantine splitting and error reporting",
    )
    return parser.parse_args(args)


def run_spark_pipeline(
    storage_root: str = "data",
    review_slots: list[str] | None = None,
    mode: str = "replay",
    inject_quarantine_test: bool = False,
    spark: SparkSession | None = None,
) -> dict:
    """Executes the full PySpark ETL pipeline."""
    slots = review_slots or ["2026-11-09T07:00:00Z", "2026-11-09T08:00:00Z"]
    spark_session = spark or get_spark_session("AirportWeatherReviewSparkPipeline")

    synthetic_info = None
    if inject_quarantine_test:
        synthetic_info = inject_synthetic_quarantine_batch(f"{storage_root}/raw/synthetic_quarantine")

    # 1. Read Raw JSON Envelopes
    metar_path = f"{storage_root}/raw/metar/date=*/*.json"
    forecast_path = f"{storage_root}/raw/open_meteo/date=*/*.json"
    himawari_path = f"{storage_root}/raw/himawari_metrics/date=*/*.json"

    raw_metar_df = read_raw_metar(spark_session, metar_path)
    raw_forecast_df = read_raw_open_meteo(spark_session, forecast_path)
    raw_himawari_df = read_raw_himawari_metrics(spark_session, himawari_path)

    # If synthetic batch injected, union synthetic records for quarantine verification
    if inject_quarantine_test and synthetic_info:
        synth_metar = read_raw_metar(spark_session, synthetic_info["metar_path"])
        synth_hima = read_raw_himawari_metrics(spark_session, synthetic_info["himawari_path"])
        raw_metar_df = raw_metar_df.unionByName(synth_metar)
        raw_himawari_df = raw_himawari_df.unionByName(synth_hima)

    # 2. Quality Validation, Quarantine & Revision Deduplication
    valid_metar_df, q_metar_df, rec_metar = validate_and_split_metar(raw_metar_df)
    valid_forecast_df, q_forecast_df, rec_forecast = validate_and_split_open_meteo(raw_forecast_df)
    valid_himawari_df, q_himawari_df, rec_himawari = validate_and_split_himawari(raw_himawari_df)

    quarantine_all_df = q_metar_df.unionByName(q_forecast_df).unionByName(q_himawari_df)

    # 3. Meteorological Transformations & Categorization
    enriched_metar_df = enrich_valid_metar(valid_metar_df)
    enriched_himawari_df = enrich_valid_himawari(valid_himawari_df)

    # 4. Point-in-Time Temporal Join
    snapshot_df = build_point_in_time_snapshot(
        spark=spark_session,
        review_slots_utc=slots,
        metar_df=enriched_metar_df,
        forecast_df=valid_forecast_df,
        himawari_df=enriched_himawari_df,
        airports=["VVDN", "VVNB", "VVTS"],
        mode=mode,
    )

    # 5. Priority Rules & Co-Priority Resolution
    decision_df = apply_priority_rules(snapshot_df)

    # 6. Save Processed Datasets
    processed_root = Path(storage_root) / "processed"
    processed_root.mkdir(parents=True, exist_ok=True)

    reconcile_report = build_reconciliation_report(
        metrics_by_source=[rec_metar, rec_forecast, rec_himawari],
        file_error_log_path=f"{storage_root}/logs/himawari_file_errors.jsonl",
    )

    rec_json_path = processed_root / "reconciliation" / "reconciliation_summary.json"
    rec_json_path.parent.mkdir(parents=True, exist_ok=True)
    rec_json_path.write_text(json.dumps(reconcile_report, indent=2), encoding="utf-8")

    # Export decision table and quarantine to JSONL for fast inspector access & Streamlit
    dec_jsonl_path = processed_root / "decision_table" / "decision_table.jsonl"
    dec_jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    rows = decision_df.collect()
    with dec_jsonl_path.open("w", encoding="utf-8") as f:
        for r in rows:
            d = r.asDict(recursive=True)
            # Serialize datetimes to ISO strings
            for k, v in d.items():
                if hasattr(v, "isoformat"):
                    d[k] = v.isoformat()
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    q_jsonl_path = processed_root / "quarantine" / "quarantine_records.jsonl"
    q_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    q_rows = quarantine_all_df.collect()
    with q_jsonl_path.open("w", encoding="utf-8") as f:
        for r in q_rows:
            d = r.asDict(recursive=True)
            for k, v in d.items():
                if hasattr(v, "isoformat"):
                    d[k] = v.isoformat()
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    return {
        "status": "SUCCESS",
        "review_slots": slots,
        "mode": mode,
        "decision_rows_count": len(rows),
        "quarantine_rows_count": len(q_rows),
        "all_sources_reconciled": reconcile_report["all_sources_reconciled"],
        "reconciliation": reconcile_report,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def main():
    args = parse_args()
    slots = [s.strip() for s in args.review_slots.split(",") if s.strip()]
    result = run_spark_pipeline(
        storage_root=args.storage_root,
        review_slots=slots,
        mode=args.mode,
        inject_quarantine_test=args.inject_quarantine_test,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
