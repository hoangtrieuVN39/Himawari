"""Long-lived Spark runner: one SparkSession, re-run the batch pipeline whenever Bronze changes.

Bronze daily shards are rewritten in place (same path), so Spark's file-streaming source would miss
appended records; a scheduled batch read of the active layout sees every committed write.
"""

from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from src.ingestion.fs_utils import atomic_write_bytes
from src.ingestion.raw_store import RawEnvelopeStore
from src.spark.lake_paths import default_lake_root
from src.spark.session import get_spark_session

HEARTBEAT_FILENAME = "_runner_heartbeat.json"


def heartbeat_path(storage_root: str) -> Path:
    """Local heartbeat file (HDFS-backed runs keep it under data/lake)."""
    local_root = "data" if "://" in str(storage_root) else storage_root
    return Path(default_lake_root(local_root)) / HEARTBEAT_FILENAME


def read_heartbeat(storage_root: str) -> dict | None:
    """Last heartbeat written by a runner, or None."""
    path = heartbeat_path(storage_root)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_heartbeat(storage_root: str, state: dict) -> None:
    path = heartbeat_path(storage_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(path, json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def bronze_signature(storage_root: str):
    """Cheap change detector for local Bronze; None for remote roots (re-run every poll)."""
    if "://" in str(storage_root):
        return None
    return RawEnvelopeStore(storage_root=storage_root).signature()


def run_forever(
    storage_root: str = "data",
    lake_root: str | None = None,
    review_slots: str = "all",
    mode: str = "replay",
    poll_seconds: float = 15.0,
    max_cycles: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Polls Bronze and re-runs the pipeline on change. Returns the number of pipeline runs."""
    from src.spark.run_pipeline import run_spark_pipeline

    spark = get_spark_session("AirportWeatherReviewRunner")
    state = {
        "pid_started_at_utc": _now_iso(),
        "status": "STARTING",
        "poll_seconds": poll_seconds,
        "last_check_at_utc": None,
        "last_cycle_started_at_utc": None,
        "last_cycle_completed_at_utc": None,
        "cycle_seconds": None,
        "last_run_id": None,
        "last_error": None,
        "runs": 0,
    }
    _write_heartbeat(storage_root, state)
    last_sig = object()  # never equal to a real signature: first poll always runs
    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        cycles += 1
        sig = bronze_signature(storage_root)
        state["last_check_at_utc"] = _now_iso()
        if sig is None or sig != last_sig:
            state["status"] = "RUNNING"
            state["last_cycle_started_at_utc"] = _now_iso()
            _write_heartbeat(storage_root, state)
            t0 = time.perf_counter()
            try:
                result = run_spark_pipeline(
                    storage_root=storage_root,
                    review_slots=review_slots,
                    mode=mode,
                    spark=spark,
                    lake_root=lake_root,
                )
                state.update(status="IDLE", last_run_id=result["run_id"], last_error=None)
                last_sig = sig  # signature taken before the run: writes during the run trigger another
            except Exception as exc:  # keep serving; the error is visible in the heartbeat
                state.update(status="ERROR", last_error=f"{type(exc).__name__}: {exc}")
                traceback.print_exc()
            finally:
                spark.catalog.clearCache()  # validation caches must not accumulate across cycles
            state["runs"] += 1
            state["cycle_seconds"] = round(time.perf_counter() - t0, 1)
            state["last_cycle_completed_at_utc"] = _now_iso()
            print(json.dumps({k: state[k] for k in ("status", "last_run_id", "cycle_seconds")}), flush=True)
        _write_heartbeat(storage_root, state)
        if max_cycles is None or cycles < max_cycles:
            sleep(poll_seconds)
    return state["runs"]
