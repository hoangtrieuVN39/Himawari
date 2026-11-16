"""End-to-end PySpark ETL pipeline: Bronze JSONL -> Silver/Gold Parquet lake (local or HDFS)."""

import argparse
import json
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.ingestion.raw_store import RawEnvelopeStore
from src.spark.lake_paths import default_lake_root, new_run_id
from src.spark.priority_engine import apply_priority_rules
from src.spark.replay_slots import load_replay_slots
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
from src.spark.writers import read_table, write_table_version

AIRPORTS = ["VVDN", "VVNB", "VVTS"]
SLOT_POLICIES = ("auto", "latest", "all")
BRONZE_SOURCES = ("metar", "open_meteo", "himawari_metrics")
_SLOT_SECONDS = 300  # live review slots = METAR fetch time rounded up to 5 minutes


def _is_remote(uri: str) -> bool:
    return "://" in str(uri)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parses CLI arguments for PySpark processing pipeline."""
    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline for Airport Weather Review")
    parser.add_argument(
        "--storage-root",
        type=str,
        default="data",
        help="Bronze root: local directory or HDFS URI (e.g. hdfs://namenode:9000/data)",
    )
    parser.add_argument(
        "--lake-root",
        type=str,
        default=None,
        help="Silver/Gold Parquet lake root, local or hdfs:// (default: <storage-root>/lake)",
    )
    parser.add_argument(
        "--review-slots",
        type=str,
        default="all",
        help="'all' (replay + every live slot), 'auto' (replay + latest live slot) or a comma list of ISO UTC slots",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["live", "replay"],
        default="replay",
        help="'live' requires fetched_at <= slot for METAR/B13; 'replay' also accepts back-filled observations",
    )
    parser.add_argument(
        "--inject-quarantine-test",
        action="store_true",
        help="Inject synthetic invalid test records to verify quarantine splitting and error reporting",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Keep one SparkSession alive and re-run whenever Bronze changes (long-lived runner)",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=15.0,
        help="--loop: seconds between Bronze change checks",
    )
    return parser.parse_args(args)


def _bronze_frames(spark: SparkSession, storage_root: str, empty_path: Path) -> dict[str, DataFrame]:
    """Reads the 3 Bronze sources. Local roots use the store's active layout; hdfs:// roots read the
    daily_jsonl layout that `--sync-hdfs` mirrors to HDFS."""
    readers = {
        "metar": read_raw_metar,
        "open_meteo": read_raw_open_meteo,
        "himawari_metrics": read_raw_himawari_metrics,
    }
    frames = {}
    if _is_remote(storage_root):
        root = storage_root.rstrip("/")
        for source, reader in readers.items():
            frames[source] = reader(spark, [f"{root}/raw_compact/{source}/date=*/*.jsonl"], multiline=False)
        return frames

    store = RawEnvelopeStore(storage_root=storage_root)
    for source, reader in readers.items():
        spec = store.read_specs(source)[0]
        if spec.paths:
            frames[source] = reader(spark, list(spec.paths), multiline=spec.multiline)
        else:
            frames[source] = reader(spark, str(empty_path), multiline=False)
    return frames


def discover_live_slots(raw_metar_df: DataFrame) -> list[str]:
    """Live review slots: every non-backfill METAR fetch time rounded up to the next 5 minutes."""
    rounded = F.timestamp_seconds(F.ceil(F.unix_timestamp("fetched_at_utc") / _SLOT_SECONDS) * _SLOT_SECONDS)
    rows = (
        raw_metar_df.filter(F.col("fetched_at_utc").isNotNull() & (~F.col("is_backfill")))
        .select(F.date_format(rounded, "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("slot"))
        .distinct()
        .collect()
    )
    return sorted(r["slot"] for r in rows)


def resolve_review_slots(policy_or_list: str | list[str], raw_metar_df: DataFrame) -> list[str]:
    """'all' -> replay + every live slot; 'auto'/'latest' -> replay + newest live slot; else explicit list."""
    if isinstance(policy_or_list, list):
        return sorted(set(policy_or_list))
    arg = policy_or_list.strip()
    if arg.lower() not in SLOT_POLICIES:
        return sorted({s.strip() for s in arg.split(",") if s.strip()})
    slots = set(load_replay_slots())
    live = discover_live_slots(raw_metar_df)
    if arg.lower() == "all":
        slots.update(live)
    elif live:
        slots.add(live[-1])
    return sorted(slots)


def run_spark_pipeline(
    storage_root: str = "data",
    review_slots: str | list[str] | None = None,
    mode: str = "replay",
    inject_quarantine_test: bool = False,
    spark: SparkSession | None = None,
    lake_root: str | None = None,
) -> dict:
    """Executes the full pipeline and commits Silver/Gold snapshot versions to the lake."""
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    spark_session = spark or get_spark_session("AirportWeatherReviewSparkPipeline")
    timings = {"session_init": time.perf_counter() - t0}
    lake = lake_root or default_lake_root(storage_root)

    synthetic_info = None
    if inject_quarantine_test:
        if _is_remote(storage_root):
            raise ValueError("--inject-quarantine-test needs a local storage root")
        synthetic_info = inject_synthetic_quarantine_batch(f"{storage_root}/raw/synthetic_quarantine")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        empty_path = Path(tmp_dir) / "empty.jsonl"
        empty_path.write_bytes(b"")  # zero-row input that keeps the reader's output schema
        frames = _bronze_frames(spark_session, storage_root, empty_path)
        if synthetic_info:
            frames["metar"] = frames["metar"].unionByName(read_raw_metar(spark_session, synthetic_info["metar_path"]))
            frames["himawari_metrics"] = frames["himawari_metrics"].unionByName(
                read_raw_himawari_metrics(spark_session, synthetic_info["himawari_path"])
            )
        slots = resolve_review_slots(review_slots if review_slots is not None else "all", frames["metar"])
        result = _run_stages(
            spark_session=spark_session,
            storage_root=storage_root,
            lake_root=lake,
            slots=slots,
            mode=mode,
            frames=frames,
            timings=timings,
        )

    timings["total"] = time.perf_counter() - t0
    result["timings_s"] = {k: round(v, 3) for k, v in timings.items()}
    result["started_at_utc"] = _iso(started)
    _append_run_log(storage_root, result)
    return result


def _run_stages(
    *,
    spark_session: SparkSession,
    storage_root: str,
    lake_root: str,
    slots: list[str],
    mode: str,
    frames: dict[str, DataFrame],
    timings: dict[str, float],
) -> dict:
    """Validation, enrichment, point-in-time join, priority rules and lake commit."""
    t = time.perf_counter()
    # 1. Quality validation, quarantine & revision dedup (triggers the reconciliation counts)
    valid_metar_df, q_metar_df, rec_metar = validate_and_split_metar(frames["metar"])
    valid_forecast_df, q_forecast_df, rec_forecast = validate_and_split_open_meteo(frames["open_meteo"])
    valid_himawari_df, q_himawari_df, rec_himawari = validate_and_split_himawari(frames["himawari_metrics"])
    quarantine_all_df = q_metar_df.unionByName(q_forecast_df).unionByName(q_himawari_df)
    timings["read_validate"] = time.perf_counter() - t

    # 2. Enrichment, point-in-time join, Level 1-4 rules
    t = time.perf_counter()
    enriched_metar_df = enrich_valid_metar(valid_metar_df)
    enriched_himawari_df = enrich_valid_himawari(valid_himawari_df)
    snapshot_df = build_point_in_time_snapshot(
        spark=spark_session,
        review_slots_utc=slots,
        metar_df=enriched_metar_df,
        forecast_df=valid_forecast_df,
        himawari_df=enriched_himawari_df,
        airports=AIRPORTS,
        mode=mode,
    )
    decision_df = apply_priority_rules(snapshot_df).cache()
    decision_rows = decision_df.count()
    timings["transform_join_priority"] = time.perf_counter() - t

    # 3. Commit Silver + Gold versions
    t = time.perf_counter()
    run_id = new_run_id()
    published_at = datetime.now(timezone.utc)
    replay = set(load_replay_slots())
    gold_df = _with_publication_columns(decision_df, lake_root, replay, published_at, run_id).cache()

    write_table_version(
        enriched_metar_df.withColumn("date", F.to_date("observed_at_utc")),
        lake_root, "metar_observations", run_id, ["date"],
    )
    write_table_version(
        valid_forecast_df.withColumn("date", F.to_date("fetched_at_utc")),
        lake_root, "open_meteo_forecasts", run_id, ["date"],
    )
    write_table_version(
        enriched_himawari_df.withColumn("date", F.to_date("scan_time_utc")),
        lake_root, "himawari_b13_metrics", run_id, ["date"],
    )
    write_table_version(quarantine_all_df, lake_root, "quarantine", run_id)
    write_table_version(gold_df, lake_root, "decision_table", run_id, ["review_date"])

    file_error_log = (
        f"{storage_root}/logs/himawari_file_errors.jsonl"
        if not _is_remote(storage_root)
        else "data/logs/himawari_file_errors.jsonl"
    )
    reconcile_report = build_reconciliation_report(
        metrics_by_source=[rec_metar, rec_forecast, rec_himawari],
        file_error_log_path=file_error_log,
    )
    write_table_version(
        _reconciliation_df(spark_session, reconcile_report, run_id), lake_root, "reconciliation", run_id
    )
    quarantine_rows = quarantine_all_df.count()
    latest_live = _latest_live_inputs(gold_df)
    gold_df.unpersist()
    decision_df.unpersist()
    timings["write"] = time.perf_counter() - t

    completed = datetime.now(timezone.utc)
    latency_s = None
    if latest_live["inputs_complete_at_utc"]:
        inputs_dt = datetime.fromisoformat(latest_live["inputs_complete_at_utc"].replace("Z", "+00:00"))
        latency_s = round((completed - inputs_dt).total_seconds(), 1)
    return {
        "status": "SUCCESS",
        "run_id": run_id,
        "lake_root": lake_root,
        "review_slots": slots,
        "review_slot_count": len(slots),
        "mode": mode,
        "decision_rows_count": decision_rows,
        "quarantine_rows_count": quarantine_rows,
        "all_sources_reconciled": reconcile_report["all_sources_reconciled"],
        "reconciliation": reconcile_report,
        "latest_live_slot_utc": latest_live["slot"],
        "latest_inputs_complete_at_utc": latest_live["inputs_complete_at_utc"],
        "latency_s": latency_s,
        "completed_at_utc": _iso(completed),
    }


def _with_publication_columns(
    decision_df: DataFrame,
    lake_root: str,
    replay_slots: set[str],
    published_at: datetime,
    run_id: str,
) -> DataFrame:
    """Adds slot kind, input completeness time and first publication time (kept across runs)."""
    slot_str = F.date_format("review_slot_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    # Parsed from an explicit UTC string: Python datetime literals are shifted by the OS timezone.
    published_lit = F.to_timestamp(F.lit(_iso(published_at)), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    out = (
        decision_df.withColumn(
            "slot_kind",
            F.when(slot_str.isin(sorted(replay_slots)), F.lit("replay")).otherwise(F.lit("live")),
        )
        .withColumn(
            "inputs_complete_at_utc",
            F.greatest("metar_fetched_at_utc", "forecast_fetched_at_utc", "b13_fetched_at_utc"),
        )
        .withColumn("published_at_utc", published_lit)
        .withColumn("run_id", F.lit(run_id))
        .withColumn("review_date", F.to_date("review_slot_utc"))
    )
    previous = read_table(decision_df.sparkSession, lake_root, "decision_table")
    if previous is not None and "first_published_at_utc" in previous.columns:
        prev = previous.select(
            "review_slot_utc", "airport_icao", F.col("first_published_at_utc").alias("_prev_first")
        )
        out = out.join(prev, on=["review_slot_utc", "airport_icao"], how="left")
        out = out.withColumn("first_published_at_utc", F.coalesce("_prev_first", published_lit)).drop("_prev_first")
    else:
        out = out.withColumn("first_published_at_utc", published_lit)
    return out


def _latest_live_inputs(gold_df: DataFrame) -> dict:
    """Newest live slot and the time its inputs were complete (for publication latency)."""
    row = (
        gold_df.filter(F.col("slot_kind") == "live")
        .groupBy("review_slot_utc")
        .agg(F.max("inputs_complete_at_utc").alias("inputs_complete"))
        .orderBy(F.col("review_slot_utc").desc())
        .limit(1)
        .select(
            F.date_format("review_slot_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("slot"),
            F.date_format("inputs_complete", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("inputs_complete_at_utc"),
        )
        .collect()
    )
    if not row:
        return {"slot": None, "inputs_complete_at_utc": None}
    return {"slot": row[0]["slot"], "inputs_complete_at_utc": row[0]["inputs_complete_at_utc"]}


def _reconciliation_df(spark: SparkSession, report: dict, run_id: str) -> DataFrame:
    """One row per source with the conservation counts of this run (literals, no Python RDD)."""
    binary = report["binary_file_errors"]
    structs = [
        F.struct(
            F.lit(run_id).alias("run_id"),
            F.lit(report["generated_at_utc"]).alias("generated_at_utc"),
            F.lit(bool(report["all_sources_reconciled"])).alias("all_sources_reconciled"),
            F.lit(src["source"]).alias("source"),
            F.lit(int(src["rows_read"])).cast("long").alias("rows_read"),
            F.lit(int(src["rows_quarantined"])).cast("long").alias("rows_quarantined"),
            F.lit(int(src["rows_valid_pre_dedup"])).cast("long").alias("rows_valid_pre_dedup"),
            F.lit(int(src["rows_retained_valid"])).cast("long").alias("rows_retained_valid"),
            F.lit(int(src["rows_duplicate_dropped"])).cast("long").alias("rows_duplicate_dropped"),
            F.lit(bool(src["is_reconciled"])).alias("is_reconciled"),
            F.lit(int(binary["total_count"])).cast("long").alias("binary_errors_total"),
            F.lit(int(binary["synthetic_count"])).cast("long").alias("binary_errors_synthetic"),
            F.lit(str(binary["log_path"])).alias("binary_errors_log_path"),
        )
        for src in report["sources"]
    ]
    return spark.range(1).select(F.inline(F.array(*structs)))


def _append_run_log(storage_root: str, result: dict) -> None:
    """Appends one line per run to <storage_root>/logs/pipeline_runs.jsonl (data/logs for HDFS roots)."""
    log_dir = Path("data/logs") if _is_remote(storage_root) else Path(storage_root) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    entry = {
        "run_id": result["run_id"],
        "started_at_utc": result["started_at_utc"],
        "completed_at_utc": result["completed_at_utc"],
        "mode": result["mode"],
        "lake_root": result["lake_root"],
        "review_slot_count": result["review_slot_count"],
        "decision_rows": result["decision_rows_count"],
        "quarantine_rows": result["quarantine_rows_count"],
        "all_sources_reconciled": result["all_sources_reconciled"],
        "latest_live_slot_utc": result["latest_live_slot_utc"],
        "latest_inputs_complete_at_utc": result["latest_inputs_complete_at_utc"],
        "latency_s": result["latency_s"],
        "timings_s": result["timings_s"],
    }
    with (log_dir / "pipeline_runs.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main():
    args = parse_args()
    if args.loop:
        from src.spark.runner import run_forever

        run_forever(
            storage_root=args.storage_root,
            lake_root=args.lake_root,
            review_slots=args.review_slots,
            mode=args.mode,
            poll_seconds=args.poll_seconds,
        )
        return
    result = run_spark_pipeline(
        storage_root=args.storage_root,
        review_slots=args.review_slots,
        mode=args.mode,
        inject_quarantine_test=args.inject_quarantine_test,
        lake_root=args.lake_root,
    )
    print(json.dumps({k: v for k, v in result.items() if k != "review_slots"}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
