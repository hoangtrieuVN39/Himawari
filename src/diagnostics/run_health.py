"""CLI/orchestrator: computes every health metric and writes Gold Parquet + summary.json.

    python -m src.diagnostics.run_health --storage-root data [--window-hours 24] [--check-source]

Outputs (overwritten atomically) under ``<storage_root>/lake/gold/health/``: one ``<table>.parquet`` per
entry of ``health.TABLE_SCHEMAS`` plus ``summary.json`` (what the dashboard tab reads).
"""

from __future__ import annotations

import argparse
import json
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional

from src.diagnostics import anomaly as anomaly_mod
from src.diagnostics import drift as drift_mod
from src.diagnostics import gap_attribution as gap_mod
from src.diagnostics import health as h
from src.diagnostics import profile as profile_mod
from src.ingestion.storage import append_ingestion_log

HEALTH_SUBDIR = Path("lake") / "gold" / "health"
SUMMARY_FILENAME = "summary.json"
RUN_LOG_FILENAME = "health_runs.jsonl"
ANOMALY_SUMMARY_LIMIT = 50


def health_dir(storage_root: str | Path) -> Path:
    return Path(storage_root) / HEALTH_SUBDIR


def _guarded(name: str, errors: list[dict[str, str]], fn: Callable[[], Any], default: Any) -> Any:
    """Runs one section; a failure is recorded in summary['errors'] and the other sections still run."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - diagnostics must degrade per section, never abort wholesale
        errors.append({"section": name, "error": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc(limit=3)})
        return default


def run_health(
    storage_root: str = "data",
    window_hours: int = 24,
    check_source: bool = False,
    *,
    now: Optional[datetime] = None,
    checker: Optional[gap_mod.SourceChecker] = None,
    config_path: Optional[str] = None,
    profile_days: int = 7,
    max_source_checks: int = gap_mod.DEFAULT_MAX_SOURCE_CHECKS,
) -> dict[str, Any]:
    """Computes all metrics, writes the tables + summary.json and returns the summary dict."""
    t0 = time.monotonic()
    now = (now or datetime.now(h.UTC)).astimezone(h.UTC).replace(microsecond=0)
    start = now - timedelta(hours=window_hours)
    end = now
    errors: list[dict[str, str]] = []
    if check_source and checker is None:
        checker = gap_mod.S3HeadChecker()

    airports = h.load_airports(config_path)
    entries = _guarded("ingestion_log", errors, lambda: h.load_ingestion_log(storage_root), [])
    tables: dict[str, list[dict[str, Any]]] = {name: [] for name in h.TABLE_SCHEMAS}
    metrics = tables["metrics"]

    # Himawari evidence → coverage + gap attribution
    segments, airport_segment = h.required_segments(airports)
    evidence = _guarded(
        "himawari_evidence", errors,
        lambda: h.collect_himawari_evidence(storage_root, airports, entries, start - timedelta(days=31), end),
        h.HimawariEvidence(segments=segments, airport_segment=airport_segment),
    )
    himawari_latest = h.latest_himawari_scans(evidence)
    cov_days, cov_metrics = _guarded(
        "himawari_coverage", errors, lambda: h.compute_himawari_coverage(evidence, now, start, end), ([], [])
    )
    tables["himawari_coverage"] = cov_days
    metrics += cov_metrics

    # Freshness
    metar_obs = _guarded("metar_observations", errors, lambda: h.latest_metar_obs(storage_root, now), {})
    fresh_rows, hourly_rows, fresh_metrics = _guarded(
        "freshness", errors,
        lambda: h.compute_freshness(entries, metar_obs, himawari_latest, now, start, end), ([], [], []),
    )
    tables["freshness"], tables["freshness_hourly"] = fresh_rows, hourly_rows
    metrics += fresh_metrics

    # Gap attribution
    window_slots = h.expected_himawari_slots(start, end, now)

    def _gaps() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        rows = gap_mod.classify_himawari_gaps(evidence, window_slots, start, end, checker, max_source_checks)
        stats: dict[str, dict[str, Any]] = {}
        for source in ("metar", "open_meteo"):
            src_rows, st = gap_mod.classify_cycle_gaps(entries, source, start, end)
            rows += src_rows
            stats[source] = st
        return rows, stats

    gap_rows, cycle_stats = _guarded("gap_attribution", errors, _gaps, ([], {}))
    tables["gap_attribution"] = gap_rows
    metar_obs_stats = _guarded("metar_observation_stats", errors, lambda: gap_mod.metar_cycle_observation_stats(storage_root, start, end), None)
    tables["gap_summary"] = gap_mod.summarize_gaps(gap_rows, start, end, checker is not None, metar_obs_stats)
    fetch_times: dict[str, list[datetime]] = {}
    for e in entries:
        if not e.get("is_backfill") and start <= e["_t"] <= end:
            fetch_times.setdefault(str(e.get("source")), []).append(e["_t"])
    metrics += gap_mod.gap_metrics(gap_rows, window_slots, cycle_stats, start, end, fetch_times)
    if metar_obs_stats:
        metrics.append(h.make_metric(
            "metar_cycles_with_new_observation", "metar", metar_obs_stats["with_new_observation"],
            metar_obs_stats["fetched_cycles"], start, end, [],
            detail={"note": "METAR is issued roughly hourly; most 10-min cycles legitimately bring nothing new"},
        ))

    # Decision KPIs + pipeline latency
    def _kpis() -> list[dict[str, Any]]:
        from src.dashboard.data_loader import load_decision_slots

        return h.compute_decision_kpis(load_decision_slots(storage_root), start, end)

    metrics += _guarded("decision_kpis", errors, _kpis, [])
    runs = _guarded("pipeline_runs", errors, lambda: h.load_pipeline_runs(storage_root), [])
    lat_metrics, stage_rows = h.compute_pipeline_latency(runs, start, end)
    metrics += lat_metrics
    tables["pipeline_stage_timings"] = stage_rows

    # Anomalies, drift, profile, volume
    tables["anomalies"] = _guarded("anomalies", errors, lambda: anomaly_mod.detect_anomalies(entries, start, end), [])
    features = _guarded("drift_features", errors, lambda: drift_mod.collect_drift_features(storage_root, now), {})
    tables["drift"] = _guarded("drift", errors, lambda: drift_mod.compute_drift(features, now), [])
    tables["profile"] = _guarded("profile", errors, lambda: profile_mod.compute_profiles(storage_root, now, profile_days), [])
    tables["volume"] = _guarded("volume", errors, lambda: h.compute_volume(storage_root, entries, start, end), [])

    heartbeat = h.read_heartbeat(storage_root, now)
    if heartbeat.get("present"):
        metrics.append(h.make_metric(
            "runner_heartbeat_age_minutes", "ALL", heartbeat.get("age_minutes") or 0.0,
            heartbeat.get("stale_threshold_minutes") or 0.0, start, end, [], unit="minutes",
            value=heartbeat.get("age_minutes"), status="stale" if heartbeat.get("is_stale") else "ok",
            detail={"status": heartbeat.get("status")},
        ))

    out_dir = health_dir(storage_root)
    for name, rows in tables.items():
        h.write_parquet_atomic(out_dir / f"{name}.parquet", rows, h.TABLE_SCHEMAS[name])

    flagged = [r for r in tables["anomalies"] if r["is_anomaly"]]
    summary: dict[str, Any] = {
        "generated_at_utc": h.iso_z(now),
        "storage_root": str(storage_root),
        "window": {"hours": window_hours, "start_utc": h.iso_z(start), "end_utc": h.iso_z(end)},
        "source_check": checker is not None,
        "metrics": metrics,
        "freshness": tables["freshness"],
        "coverage_all_segments": [r for r in tables["himawari_coverage"] if r["scope_type"] == "all_segments"],
        "gap_summary": tables["gap_summary"],
        "anomaly_counts": {"rows": len(tables["anomalies"]), "flagged": len(flagged)},
        "anomalies_flagged": flagged[-ANOMALY_SUMMARY_LIMIT:],
        "drift": tables["drift"],
        "volume": tables["volume"],
        "pipeline_stage_timings": stage_rows,
        "heartbeat": heartbeat,
        "tables": {name: len(rows) for name, rows in tables.items()},
        "errors": errors,
    }
    summary["duration_s"] = round(time.monotonic() - t0, 2)
    h.write_json_atomic(out_dir / SUMMARY_FILENAME, summary)
    append_ingestion_log(
        {"generated_at_utc": summary["generated_at_utc"], "window_hours": window_hours,
         "source_check": summary["source_check"], "duration_s": summary["duration_s"],
         "tables": summary["tables"], "errors": [e["section"] for e in errors]},
        log_path=str(Path(storage_root) / "logs" / RUN_LOG_FILENAME),
    )
    return summary


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Tính chỉ số sức khỏe vận hành (Gold health/).")
    p.add_argument("--storage-root", default="data")
    p.add_argument("--window-hours", type=int, default=24)
    p.add_argument("--check-source", action="store_true",
                   help="HEAD/list S3 công khai để phân loại SOURCE_MISSING vs RECEIVER_FAULT")
    p.add_argument("--profile-days", type=int, default=7)
    p.add_argument("--max-source-checks", type=int, default=gap_mod.DEFAULT_MAX_SOURCE_CHECKS)
    p.add_argument("--config", default=None, help="đường dẫn airports.json (mặc định config/airports.json)")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    summary = run_health(
        args.storage_root, args.window_hours, args.check_source,
        config_path=args.config, profile_days=args.profile_days, max_source_checks=args.max_source_checks,
    )
    print(json.dumps({k: summary[k] for k in ("generated_at_utc", "window", "source_check", "tables", "duration_s")},
                     ensure_ascii=False, indent=2))
    for m in summary["metrics"]:
        if m["scope"] in ("ALL", "window") or m["name"] in ("hourly_availability", "ingest_cycle_completeness"):
            val = "n/a" if m["value"] is None else f"{m['value']:.4g}"
            print(f"{m['name']}[{m['scope']}] = {val} ({m['numerator']:g}/{m['denominator']:g}, "
                  f"missing_hours={m['missing_hours_count']}, {m['status']})")
    if summary["errors"]:
        print("SECTION ERRORS:", [e["section"] for e in summary["errors"]])
    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
