"""Distributed Himawari-9 B13 backfill: Satpy slot processing fanned out over Spark partitions.

Each Spark task runs `src.ingestion.run_ingest.run_himawari_slot_pipeline(..., is_backfill=True)` for a contiguous
batch of 10-minute slots. Both segments derived from `config/airports.json` (S0410 for VVDN/VVNB, S0510 for VVTS)
are ALWAYS downloaded: dropping S0510 would silently remove VVTS from every satellite feature.

Disk stays bounded: right after a slot is processed (metrics envelope + PNGs written) its raw `.DAT.bz2` files are
deleted, except slots pinned in `config/replay_slots.json`.

CLI:
    python -m src.spark.himawari_batch --start 2026-11-15T06:00:00Z --end 2026-11-15T06:50:00Z --parallelism 4
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SLOT_MINUTES = 10
_SLOT_FMT = "%Y-%m-%dT%H:%M:%SZ"
BACKFILL_LOG_NAME = "backfill_log.jsonl"


# --------------------------------------------------------------------------- slot helpers


def _parse_iso_utc(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        dt = datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def list_slots(start_iso: str, end_iso: str) -> List[str]:
    """Returns every 10-minute Himawari scan slot within [start, end] (inclusive) as ISO-Z strings.

    Scans happen on :00/:10/:20/..., so `start` is rounded up and `end` rounded down to that grid.
    """
    start = _parse_iso_utc(start_iso)
    end = _parse_iso_utc(end_iso)
    step = timedelta(minutes=SLOT_MINUTES)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    first = epoch + -((epoch - start) // step) * step  # ceil to the 10-minute grid
    slots: List[str] = []
    current = first
    while current <= end:
        slots.append(current.strftime(_SLOT_FMT))
        current += step
    return slots


def required_segments(airports_cfg: Dict[str, Any]) -> List[str]:
    """Sorted distinct Himawari segments needed by the configured airports (e.g. ['S0410', 'S0510'])."""
    return sorted({cfg.get("himawari_segment", "S0410") for cfg in airports_cfg.values()})


def _peak_rss_mb() -> Optional[float]:
    """Peak resident set size of the current process in MiB (None if unavailable)."""
    try:
        if sys.platform == "win32":
            import ctypes
            from ctypes import wintypes

            class _PMC(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(_PMC), wintypes.DWORD]
            counters = _PMC()
            counters.cb = ctypes.sizeof(_PMC)
            if psapi.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
                return round(counters.PeakWorkingSetSize / (1024 * 1024), 1)
            return None
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024, 1)
    except Exception:  # noqa: BLE001 - diagnostics only
        return None


def _append_backfill_log(entry: Dict[str, Any], log_path: str) -> None:
    """Appends one JSON line; serialised across worker processes with a portalocker file lock."""
    import portalocker

    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    with portalocker.Lock(str(path) + ".lock", timeout=30):
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()


def check_slot_archive(
    slot_dt: datetime,
    segments: Sequence[str],
    head: Optional[Callable[..., Any]] = None,
    attempts: int = 2,
) -> Dict[str, bool]:
    """HEADs each segment on S3; True only when HTTP 200. Guards against the single-slot pipeline's
    'latest available slot' fallback, which would store another day's imagery under a historical slot."""
    import requests

    from src.ingestion.himawari_client import build_s3_segment_url

    head = head or requests.head
    available: Dict[str, bool] = {}
    for segment in segments:
        url, _ = build_s3_segment_url(slot_dt, segment=segment)
        ok = False
        for _ in range(attempts):
            try:
                if head(url, timeout=10.0).status_code == 200:
                    ok = True
                    break
                if head is requests.head:  # a definite 403/404 is not worth retrying
                    break
            except Exception:  # noqa: BLE001
                continue
        available[segment] = ok
    return available


def _cleanup_slot_raw(slot_dt: datetime, base_dir: str, pinned_slots: Iterable[str]) -> int:
    """Deletes this slot's raw `.DAT.bz2` (and stray `.tmp`) files unless the slot is pinned. Returns files removed."""
    slot_iso = slot_dt.strftime(_SLOT_FMT)
    if slot_iso in set(pinned_slots):
        return 0
    slot_dir = (
        Path(base_dir) / "raw" / "himawari_hsd"
        / f"date={slot_dt.strftime('%Y-%m-%d')}" / f"slot={slot_dt.strftime('%H%M')}"
    )
    if not slot_dir.is_dir():
        return 0
    removed = 0
    for file_path in list(slot_dir.glob("*.DAT.bz2")) + list(slot_dir.glob("*.tmp")):
        try:
            file_path.unlink()
            removed += 1
        except OSError:
            pass
    try:
        slot_dir.rmdir()
    except OSError:
        pass
    return removed


# --------------------------------------------------------------------------- worker side


def _default_slot_runner() -> Callable[..., Dict[str, Any]]:
    from src.ingestion.run_ingest import run_himawari_slot_pipeline

    return run_himawari_slot_pipeline


def process_slot_partition(
    slot_isos: Iterable[str],
    airports_cfg: Dict[str, Any],
    base_dir: str,
    pinned_slots: Sequence[str],
    retain_raw_hours: int = 24,
    log_path: Optional[str] = None,
    project_root: Optional[str] = None,
    slot_runner: Optional[Callable[..., Dict[str, Any]]] = None,
    archive_checker: Optional[Callable[[datetime, Sequence[str]], Dict[str, bool]]] = None,
) -> Iterator[Dict[str, Any]]:
    """Processes the slots of one Spark partition sequentially and yields one result dict per slot.

    Runs without a SparkSession so it is directly unit-testable and also serves as the sequential baseline.
    """
    if project_root and project_root not in sys.path:
        sys.path.insert(0, project_root)
    runner = slot_runner or _default_slot_runner()
    checker = archive_checker or check_slot_archive
    segments = required_segments(airports_cfg)

    for slot_iso in slot_isos:
        slot_dt = _parse_iso_utc(slot_iso)
        started = time.perf_counter()
        result: Dict[str, Any] = {
            "event": "slot",
            "slot_utc": slot_iso,
            "status": "FAILED",
            "segments_requested": segments,
            "segments_downloaded": [],
            "bytes_downloaded": 0,
            "metrics_statuses": {},
            "raw_files_deleted": 0,
            "worker_pid": os.getpid(),
            "error": None,
        }
        try:
            availability = checker(slot_dt, segments)
            missing = sorted(seg for seg, ok in availability.items() if not ok)
            if missing:
                result["status"] = "ARCHIVE_UNAVAILABLE"
                result["error"] = f"segments not on S3 (no fallback substitution): {missing}"
            else:
                pipeline_res = runner(
                    scan_dt_utc=slot_dt,
                    airports_cfg=airports_cfg,
                    base_dir=base_dir,
                    is_backfill=True,
                    retain_raw_hours=retain_raw_hours,
                    pinned_slots=list(pinned_slots),
                )
                download = pipeline_res.get("download") or {}
                files = download.get("files") or {}
                result["segments_downloaded"] = sorted(files)
                result["bytes_downloaded"] = int(download.get("bytes_written") or 0)
                airports = (pipeline_res.get("metrics") or {}).get("airports") or []
                statuses: Dict[str, int] = {}
                for airport in airports:
                    key = str(airport.get("status"))
                    statuses[key] = statuses.get(key, 0) + 1
                result["metrics_statuses"] = statuses
                result["status"] = pipeline_res.get("status", "UNKNOWN")
                result["source_slot_utc"] = download.get("source_slot_utc", slot_iso)
                if result["source_slot_utc"] != slot_iso:
                    result["status"] = "FALLBACK_SUBSTITUTED"
                    result["error"] = f"imagery of {result['source_slot_utc']} stored under {slot_iso}"
                elif set(files) != set(segments):
                    result["status"] = "INCOMPLETE_SEGMENTS"
                    result["error"] = f"downloaded {sorted(files)} of required {segments}"
        except Exception as err:  # noqa: BLE001 - one bad slot must not kill the partition
            result["error"] = f"{type(err).__name__}: {err}"
        finally:
            result["raw_files_deleted"] = _cleanup_slot_raw(slot_dt, base_dir, pinned_slots)
            result["seconds"] = round(time.perf_counter() - started, 3)
            result["peak_rss_mb"] = _peak_rss_mb()
        if log_path:
            _append_backfill_log(result, log_path)
        yield result


# --------------------------------------------------------------------------- driver side


def _already_processed_slots(base_dir: str, airports_cfg: Dict[str, Any]) -> set:
    """Slots whose metrics envelope already exists with every airport AVAILABLE (resume support)."""
    done: set = set()
    try:
        from src.ingestion.raw_store import RawEnvelopeStore

        expected = len(airports_cfg)
        for env in RawEnvelopeStore(storage_root=base_dir).iter_envelopes("himawari_metrics", strict=False):
            slot = (env.get("metadata") or {}).get("scan_slot_utc")
            airports = env.get("airports") or []
            if slot and len(airports) >= expected and all(a.get("status") == "AVAILABLE" for a in airports):
                done.add(slot)
    except Exception:  # noqa: BLE001 - resume is an optimisation only
        return set()
    return done


def summarize_slot_results(
    results: Sequence[Dict[str, Any]],
    wall_seconds: float,
    skipped_existing: int = 0,
) -> Dict[str, Any]:
    """Totals, throughput and per-status counts for a list of per-slot results."""
    counts: Dict[str, int] = {}
    for res in results:
        counts[res["status"]] = counts.get(res["status"], 0) + 1
    seconds = [res["seconds"] for res in results]
    peaks = [res["peak_rss_mb"] for res in results if res.get("peak_rss_mb") is not None]
    attempted = len(results)
    return {
        "slots_attempted": attempted,
        "slots_skipped_existing": skipped_existing,
        "status_counts": counts,
        "wall_seconds": round(wall_seconds, 3),
        "slots_per_min": round(attempted / (wall_seconds / 60.0), 3) if wall_seconds > 0 and attempted else 0.0,
        "bytes_downloaded": sum(res["bytes_downloaded"] for res in results),
        "mean_slot_seconds": round(statistics.fmean(seconds), 3) if seconds else None,
        "median_slot_seconds": round(statistics.median(seconds), 3) if seconds else None,
        "peak_worker_rss_mb": max(peaks) if peaks else None,
        "raw_files_deleted": sum(res["raw_files_deleted"] for res in results),
    }


def _ensure_windows_spark_env() -> None:
    hadoop_home = PROJECT_ROOT / "config" / "hadoop"
    if sys.platform == "win32" and hadoop_home.exists() and "HADOOP_HOME" not in os.environ:
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ.get("PATH", "")
    existing = os.environ.get("PYTHONPATH", "")
    if str(PROJECT_ROOT) not in existing.split(os.pathsep):
        os.environ["PYTHONPATH"] = str(PROJECT_ROOT) + (os.pathsep + existing if existing else "")


def run_distributed_himawari_backfill(
    start_iso: str,
    end_iso: str,
    parallelism: int,
    base_dir: str = "data",
    config_path: str = "config/airports.json",
    retain_raw_hours: int = 24,
    skip_existing: bool = True,
    spark: Any = None,
) -> Dict[str, Any]:
    """Backfills all 10-minute slots in [start_iso, end_iso] using `parallelism` Spark partitions/workers.

    Per-slot rows and a summary line are appended to <base_dir>/logs/backfill_log.jsonl; the same data is returned.
    """
    from src.ingestion.config_loader import load_airports_config
    from src.spark.replay_slots import load_replay_slots

    if parallelism < 1:
        raise ValueError(f"parallelism must be >= 1, got {parallelism}")

    airports_cfg = load_airports_config(config_path)
    segments = required_segments(airports_cfg)
    pinned_slots = load_replay_slots(str(PROJECT_ROOT / "config" / "replay_slots.json"))
    all_slots = list_slots(start_iso, end_iso)
    skipped = _already_processed_slots(base_dir, airports_cfg) if skip_existing else set()
    slots = [slot for slot in all_slots if slot not in skipped]
    log_path = str(Path(base_dir) / "logs" / BACKFILL_LOG_NAME)

    owns_spark = spark is None
    session_started = time.perf_counter()
    if owns_spark:
        from src.spark.session import get_spark_session

        _ensure_windows_spark_env()
        spark = get_spark_session("HimawariBackfill", master=f"local[{parallelism}]", force_new=True)
    session_seconds = time.perf_counter() - session_started

    project_root = str(PROJECT_ROOT)
    base_dir_s = str(base_dir)

    def _run_partition(slot_iter: Iterable[str]) -> Iterator[Dict[str, Any]]:
        return process_slot_partition(
            slot_iter,
            airports_cfg=airports_cfg,
            base_dir=base_dir_s,
            pinned_slots=pinned_slots,
            retain_raw_hours=retain_raw_hours,
            log_path=log_path,
            project_root=project_root,
        )

    started_at = datetime.now(timezone.utc).strftime(_SLOT_FMT)
    wall_started = time.perf_counter()
    try:
        results: List[Dict[str, Any]] = []
        if slots:
            rdd = spark.sparkContext.parallelize(slots, parallelism).mapPartitions(_run_partition)
            results = sorted(rdd.collect(), key=lambda res: res["slot_utc"])
    finally:
        if owns_spark:
            from src.spark.session import stop_spark_session

            stop_spark_session()
    wall_seconds = time.perf_counter() - wall_started

    summary = {
        "event": "summary",
        "start_utc": start_iso,
        "end_utc": end_iso,
        "started_at_utc": started_at,
        "parallelism": parallelism,
        "segments": segments,
        "slots_total": len(all_slots),
        "spark_session_seconds": round(session_seconds, 3),
        **summarize_slot_results(results, wall_seconds, skipped_existing=len(skipped & set(all_slots))),
    }
    _append_backfill_log(summary, log_path)
    return {"summary": summary, "slots": results}


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Distributed Himawari-9 B13 backfill (Satpy over Spark)")
    parser.add_argument("--start", required=True, help="Window start, ISO-8601 UTC (e.g. 2026-11-15T06:00:00Z)")
    parser.add_argument("--end", required=True, help="Window end (inclusive), ISO-8601 UTC")
    parser.add_argument("--parallelism", type=int, default=2, help="Spark partitions = concurrent Satpy workers")
    parser.add_argument("--base-dir", default="data", help="Base directory for raw data and logs")
    parser.add_argument("--config", default="config/airports.json", help="Airports configuration JSON")
    parser.add_argument("--retain-raw-hours", type=int, default=24, help="Passed to the slot pipeline cleanup")
    parser.add_argument("--no-skip-existing", action="store_true", help="Reprocess slots that already have metrics")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    outcome = run_distributed_himawari_backfill(
        start_iso=args.start,
        end_iso=args.end,
        parallelism=args.parallelism,
        base_dir=args.base_dir,
        config_path=args.config,
        retain_raw_hours=args.retain_raw_hours,
        skip_existing=not args.no_skip_existing,
    )
    print(json.dumps(outcome["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
