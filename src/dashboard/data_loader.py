"""Data loading, timestamp normalization, and audit parsing for Streamlit Dashboard.

The dashboard only READS the lake: Spark is the single source of truth for priority levels
(Gold `decision_table`), quarantine (Silver `quarantine`) and reconciliation (Gold `reconciliation`).
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any

import pyarrow.parquet as pq

from src.spark.lake_paths import default_lake_root, local_table_version
from src.spark.runner import read_heartbeat

VALID_AIRPORTS = ["VVDN", "VVNB", "VVTS"]
INGESTION_TIMEOUT_SEC = 180
PIPELINE_TIMEOUT_SEC = 300
RUNNER_WAIT_SEC = 120


def _lake_version(storage_root: str, table: str) -> Path | None:
    return local_table_version(default_lake_root(storage_root), table)


def _read_parquet_rows(version_dir: Path) -> list[dict[str, Any]]:
    """Rows of a committed lake version as plain dicts; timestamps become ISO-8601 UTC 'Z' strings."""
    table = pq.read_table(version_dir)
    rows = table.to_pylist()
    for row in rows:
        for key, value in row.items():
            if isinstance(value, datetime):
                aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
                row[key] = aware.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            elif hasattr(value, "isoformat") and not isinstance(value, str):
                row[key] = value.isoformat()
    return rows


def latest_decision_version(storage_root: str = "data") -> str | None:
    """Name of the newest committed Gold decision_table version (run_id=...), or None."""
    version = _lake_version(storage_root, "decision_table")
    return version.name if version else None


# Version served by load_decision_slots per storage root; the LIVE watcher compares against it.
_SERVED_VERSION: dict[str, str | None] = {}


def has_new_decision_version(storage_root: str = "data") -> bool:
    """True when Spark committed a newer Gold decision table than the one last loaded in this process."""
    key = str(Path(storage_root).resolve())
    if key not in _SERVED_VERSION:
        return False
    return latest_decision_version(storage_root) != _SERVED_VERSION[key]


def _runner_is_alive(storage_root: str) -> bool:
    """A long-lived Spark runner (src.spark.runner) checked Bronze recently."""
    beat = read_heartbeat(storage_root) or {}
    last = beat.get("last_check_at_utc")
    if not last or beat.get("status") == "ERROR":
        return False
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(last.replace("Z", "+00:00"))).total_seconds()
    return age <= 3 * float(beat.get("poll_seconds") or 15.0) + 60


def fetch_and_update_realtime(
    storage_root: str = "data",
    include_himawari: bool = True,
) -> dict[str, Any]:
    """Ingest METAR, Open-Meteo and Himawari-9 now, then publish a new Gold version with Spark.

    If the long-lived runner is alive it picks up the new Bronze batches itself and we wait for its
    commit; otherwise the batch pipeline runs once as a subprocess.
    """
    before = latest_decision_version(storage_root)
    cmd = [sys.executable, "-m", "src.ingestion.run_ingest", "--once", "--base-dir", storage_root]
    if include_himawari:
        cmd.append("--include-himawari")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=INGESTION_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        return {"status": "ERROR", "error": f"Ingestion timeout after {INGESTION_TIMEOUT_SEC}s"}
    except OSError as exc:
        return {"status": "ERROR", "error": f"Could not start ingestion: {exc}"}
    if proc.returncode != 0:
        return {"status": "ERROR", "error": (proc.stderr or proc.stdout or "Unknown ingestion error")[:500]}

    if _runner_is_alive(storage_root):
        deadline = time.monotonic() + RUNNER_WAIT_SEC
        while time.monotonic() < deadline and latest_decision_version(storage_root) == before:
            time.sleep(2.0)
        if latest_decision_version(storage_root) == before:
            return {"status": "ERROR", "error": f"Spark runner did not publish within {RUNNER_WAIT_SEC}s"}
        published_by = "runner"
    else:
        spark_cmd = [sys.executable, "-m", "src.spark.run_pipeline", "--storage-root", storage_root]
        try:
            proc = subprocess.run(spark_cmd, capture_output=True, text=True, timeout=PIPELINE_TIMEOUT_SEC)
        except subprocess.TimeoutExpired:
            return {"status": "ERROR", "error": f"Spark pipeline timeout after {PIPELINE_TIMEOUT_SEC}s"}
        except OSError as exc:
            return {"status": "ERROR", "error": f"Could not start Spark pipeline: {exc}"}
        if proc.returncode != 0:
            return {"status": "ERROR", "error": (proc.stderr or proc.stdout or "Spark pipeline error")[-500:]}
        published_by = "pipeline"

    slots = load_decision_slots(storage_root)
    return {
        "status": "SUCCESS",
        "published_by": published_by,
        "decision_version": latest_decision_version(storage_root),
        "latest_slot_utc": max(slots.keys()) if slots else None,
    }


def normalize_timestamp_pair(ts_str: str | None, raw_ob: str | None = None) -> dict[str, str]:
    """Normalize timestamp to both UTC and Vietnam time (ICT, UTC+7).
    
    Cross-checks the true UTC DDHHMMZ token in raw_ob (e.g. 090700Z) to correctly
    recover UTC hours/minutes even when Windows PySpark serialized UTC timestamps
    as local naive ISO strings (e.g. 14:00:00).
    
    Returns:
        dict with keys:
            - utc_iso: e.g. "2026-11-09T07:00:00Z"
            - utc_hhmm: e.g. "07:00Z"
            - vn_iso: e.g. "2026-11-09 14:00:00 ICT (UTC+7)"
            - vn_hhmm: e.g. "14:00"
            - metar_ddhhmmz: e.g. "090700Z"
    """
    utc_dt: datetime | None = None
    metar_ddhhmmz = ""

    # 1. Attempt extracting canonical METAR UTC observation token (DDHHMMZ)
    if raw_ob:
        match = re.search(r"\b(\d{2})(\d{2})(\d{2})Z\b", raw_ob)
        if match:
            day = int(match.group(1))
            hh = int(match.group(2))
            mm = int(match.group(3))
            metar_ddhhmmz = match.group(0)

            year = 2026
            month = 11
            if ts_str:
                d_match = re.match(r"^(\d{4})-(\d{2})-(\d{2})", ts_str)
                if d_match:
                    year = int(d_match.group(1))
                    month = int(d_match.group(2))

            utc_dt = datetime(year, month, day, hh, mm, 0, tzinfo=timezone.utc)

    # 2. If no valid METAR token, parse ts_str directly
    if utc_dt is None and ts_str:
        clean_ts = ts_str.strip()
        if clean_ts.endswith("Z"):
            utc_dt = datetime.fromisoformat(clean_ts.replace("Z", "+00:00"))
        elif "+" in clean_ts or clean_ts.count("-") > 2:
            parsed = datetime.fromisoformat(clean_ts)
            utc_dt = parsed.astimezone(timezone.utc)
        else:
            # Naive ISO string (e.g. "2026-11-09T14:00:00")
            naive = datetime.fromisoformat(clean_ts)
            # Heuristic for Windows PySpark collect() UTC+7 offset
            if naive.hour >= 13:
                utc_dt = (naive - timedelta(hours=7)).replace(tzinfo=timezone.utc)
            else:
                utc_dt = naive.replace(tzinfo=timezone.utc)
        metar_ddhhmmz = f"{utc_dt.day:02d}{utc_dt.hour:02d}{utc_dt.minute:02d}Z"

    # 3. Fallback default if neither raw_ob nor ts_str gave a valid datetime
    if utc_dt is None:
        utc_dt = datetime(2026, 11, 9, 7, 0, 0, tzinfo=timezone.utc)
        metar_ddhhmmz = "090700Z"

    vn_dt = utc_dt + timedelta(hours=7)

    return {
        "utc_iso": utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "utc_hhmm": utc_dt.strftime("%H:%MZ"),
        "vn_iso": f"{vn_dt.strftime('%Y-%m-%d %H:%M:%S')} ICT (UTC+7)",
        "vn_hhmm": vn_dt.strftime("%H:%M"),
        "metar_ddhhmmz": metar_ddhhmmz,
    }


# Signature caches for the read-only loaders: every Streamlit rerun calls them, but the files only change
# when ingestion/sync/pipeline writes. Key = (resolved path, mtime_ns, size) of every input file.
_LOADER_CACHE: dict[str, tuple[Any, Any]] = {}


def _file_signature(path: Path) -> tuple[str, int, int] | None:
    try:
        st = path.stat()
    except OSError:
        return None
    return (str(path.resolve()), st.st_mtime_ns, st.st_size)


def _cached(name: str, signature: Any, compute):
    hit = _LOADER_CACHE.get(name)
    if hit is not None and hit[0] == signature:
        return hit[1]
    value = compute()
    _LOADER_CACHE[name] = (signature, value)
    return value


def load_decision_slots(storage_root: str = "data") -> dict[str, list[dict[str, Any]]]:
    """Load the current Gold decision table (newest committed version) grouped by UTC slot.

    Read-only. Re-reads only when Spark commits a new version; callers get fresh dict copies so
    they cannot corrupt the cache.

    Returns:
        dict[utc_iso, list[row_dict]] where utc_iso is e.g. "2026-11-09T07:00:00Z"
    """
    version = _lake_version(storage_root, "decision_table")
    _SERVED_VERSION[str(Path(storage_root).resolve())] = version.name if version else None
    if version is None:
        return {}
    slots = _cached(f"slots:{version.parent.resolve()}", version.name, lambda: _read_decision_slots(version))
    return copy.deepcopy(slots)  # rows hold nested lists (weather codes, accepted airports)


def _read_decision_slots(version_dir: Path) -> dict[str, list[dict[str, Any]]]:
    slots: dict[str, list[dict[str, Any]]] = {}

    for row in sorted(_read_parquet_rows(version_dir), key=lambda r: (r["review_slot_utc"], r["airport_icao"])):
        row.pop("review_date", None)  # partition column, duplicated by review_slot_utc
        slot_info = normalize_timestamp_pair(row.get("review_slot_utc"))
        obs_info = normalize_timestamp_pair(row.get("observed_at_utc"), row.get("raw_ob"))

        row["slot_utc_iso"] = slot_info["utc_iso"]
        row["slot_utc_hhmm"] = slot_info["utc_hhmm"]
        row["slot_vn_iso"] = slot_info["vn_iso"]
        row["slot_vn_hhmm"] = slot_info["vn_hhmm"]
        row["slot_display_label"] = f"{slot_info['utc_hhmm']} ({slot_info['vn_hhmm']} ICT)"

        row["obs_utc_iso"] = obs_info["utc_iso"]
        row["obs_utc_hhmm"] = obs_info["utc_hhmm"]
        row["obs_vn_iso"] = obs_info["vn_iso"]
        row["obs_vn_hhmm"] = obs_info["vn_hhmm"]
        row["obs_ddhhmmz"] = obs_info["metar_ddhhmmz"]

        slots.setdefault(slot_info["utc_iso"], []).append(row)

    return slots


def load_nowcast(storage_root: str = "data") -> dict[tuple[str, str], dict[str, Any]]:
    """Current Gold `nowcast` (newest committed version) keyed by (slot UTC ISO, airport ICAO).

    Read-only and cached per committed version. Returns {} when no nowcast was ever committed. Every row
    carries `gate_passed`; the dashboard shows probabilities only when it is true.
    """
    version = _lake_version(storage_root, "nowcast")
    if version is None:
        return {}
    rows = _cached(f"nowcast:{version.parent.resolve()}", version.name, lambda: _read_nowcast(version))
    return copy.deepcopy(rows)


def _read_nowcast(version_dir: Path) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in _read_parquet_rows(version_dir):
        row.pop("review_date", None)  # partition column, duplicated by review_slot_utc
        slot = normalize_timestamp_pair(row.get("review_slot_utc"))["utc_iso"]
        out[(slot, row["airport_icao"])] = row
    return out


def sort_slot_rows(rows: list[dict[str, Any]], sort_mode: str = "priority") -> list[dict[str, Any]]:
    """Sort a list of airport rows for a given slot.
    
    Supported sort_mode values:
        - "priority": priority_level ASC, airport_icao ASC (default)
        - "icao_asc": airport_icao ASC
        - "icao_desc": airport_icao DESC (reverses co-priority rows to test tie invariance)
    """
    copied = [dict(r) for r in rows]
    if sort_mode == "priority":
        copied.sort(key=lambda r: (r.get("priority_level", 999), r.get("airport_icao", "")))
    elif sort_mode == "icao_asc":
        copied.sort(key=lambda r: r.get("airport_icao", ""))
    elif sort_mode == "icao_desc":
        copied.sort(key=lambda r: r.get("airport_icao", ""), reverse=True)
    return copied


def get_b13_image_sequence(
    storage_root: str,
    airport_icao: str,
    up_to_utc_iso: str,
    max_frames: int = 6,
    window_minutes: int = 60,
) -> list[dict[str, str]]:
    """Scan and return up to max_frames B13 PNG images chronologically <= up_to_utc_iso.
    
    Prevents future data leakage by strictly checking scan_time <= up_to_utc_iso, and only keeps
    frames from the `window_minutes` before the slot (spec: the evolution leading up to the event),
    so a slot never shows a stale frame from hours or days earlier.
    
    Returns:
        list of dict: [{"scan_time_utc": str, "utc_hhmm": str, "vn_hhmm": str, "png_path": str}]
    """
    airport_dir = Path(storage_root) / "processed" / "himawari_png" / f"airport={airport_icao}"
    if not airport_dir.exists():
        return []

    norm_target = normalize_timestamp_pair(up_to_utc_iso)
    cutoff_dt = datetime.fromisoformat(norm_target["utc_iso"].replace("Z", "+00:00"))
    window_start = cutoff_dt - timedelta(minutes=window_minutes)

    frames: list[dict[str, Any]] = []
    pattern = re.compile(r"B13_[A-Z0-9]+_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})Z\.png", re.IGNORECASE)

    for png_file in airport_dir.rglob("*.png"):
        match = pattern.search(png_file.name)
        if not match:
            continue

        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        hh = int(match.group(4))
        mm = int(match.group(5))

        dt_scan = datetime(year, month, day, hh, mm, 0, tzinfo=timezone.utc)
        if window_start <= dt_scan <= cutoff_dt:
            vn_dt = dt_scan + timedelta(hours=7)
            frames.append({
                "_dt": dt_scan,
                "scan_time_utc": dt_scan.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "utc_hhmm": f"{hh:02d}:{mm:02d}Z",
                "vn_hhmm": f"{vn_dt.hour:02d}:{vn_dt.minute:02d}",
                "png_path": str(png_file).replace("\\", "/"),
            })

    frames.sort(key=lambda x: x["_dt"])
    recent_frames = frames[-max_frames:]

    for f in recent_frames:
        f.pop("_dt", None)

    return recent_frames


def load_audit_and_reconciliation(storage_root: str = "data") -> dict[str, Any]:
    """Load reconciliation summary, quarantine records, synthetic quarantine, and operational logs.

    Cached on the lake versions and the signature of every log file; returns a deep copy.
    """
    root = Path(storage_root)
    inputs = [
        root / "logs" / "himawari_file_errors.jsonl",
        root / "logs" / "ingestion_log.jsonl",
        root / "logs" / "hdfs_sync_log.jsonl",
    ]
    syn_dir = root / "raw" / "synthetic_quarantine"
    syn_files = sorted(syn_dir.glob("*.json")) if syn_dir.exists() else []
    versions = tuple(
        str(v) if (v := _lake_version(storage_root, t)) else None for t in ("reconciliation", "quarantine")
    )
    sig = versions + tuple(_file_signature(p) for p in inputs + syn_files) + (len(syn_files),)
    value = _cached(f"audit:{root.resolve()}", sig, lambda: _read_audit_and_reconciliation(storage_root))
    return copy.deepcopy(value)


def _reconciliation_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Rebuilds the reconciliation report (one Gold row per source) in its original dict shape."""
    if not rows:
        return {"all_sources_reconciled": False, "sources": []}
    first = rows[0]
    source_keys = (
        "source", "rows_read", "rows_quarantined", "rows_valid_pre_dedup",
        "rows_retained_valid", "rows_duplicate_dropped", "is_reconciled",
    )
    return {
        "run_id": first.get("run_id"),
        "generated_at_utc": first.get("generated_at_utc"),
        "all_sources_reconciled": bool(first.get("all_sources_reconciled")),
        "sources": [{k: r.get(k) for k in source_keys} for r in rows],
        "binary_file_errors": {
            "total_count": first.get("binary_errors_total", 0),
            "synthetic_count": first.get("binary_errors_synthetic", 0),
            "log_path": first.get("binary_errors_log_path"),
        },
    }


def _read_audit_and_reconciliation(storage_root: str) -> dict[str, Any]:
    root = Path(storage_root)

    # 1. Reconciliation summary (Gold) and 2. quarantine records (Silver)
    recon_version = _lake_version(storage_root, "reconciliation")
    reconciliation = _reconciliation_from_rows(_read_parquet_rows(recon_version) if recon_version else [])
    q_version = _lake_version(storage_root, "quarantine")
    quarantine_records = _read_parquet_rows(q_version) if q_version else []
    # 3. Synthetic quarantine preview
    synthetic_preview: list[dict[str, Any]] = []
    syn_dir = root / "raw" / "synthetic_quarantine"
    if syn_dir.exists():
        for f in sorted(syn_dir.glob("*.json")):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                    meta = data.get("metadata", {})
                    if "airports" in data:
                        for ap in data["airports"]:
                            synthetic_preview.append({
                                "source": meta.get("source", "himawari_metrics"),
                                "batch_id": meta.get("batch_id"),
                                "file": f.name,
                                "airport_icao": ap.get("airport_icao"),
                                "status": ap.get("status"),
                                "error_hint": "B13 fraction > 1.0 or BT out of range",
                                "payload": ap,
                            })
                    elif "payload" in data:
                        for item in data["payload"]:
                            synthetic_preview.append({
                                "source": meta.get("source", "aviationweather_metar"),
                                "batch_id": meta.get("batch_id"),
                                "file": f.name,
                                "airport_icao": item.get("icaoId"),
                                "error_hint": "Unmonitored ICAO (VVXX), Future timestamp, or NIL METAR",
                                "payload": item,
                            })
                    else:
                        synthetic_preview.append(data)
            except Exception:
                pass

    # 4. Binary file errors (Himawari corrupted files)
    file_errors_path = root / "logs" / "himawari_file_errors.jsonl"
    binary_file_errors: list[dict[str, Any]] = []
    if file_errors_path.exists():
        with open(file_errors_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        binary_file_errors.append(json.loads(line_str))
                    except Exception:
                        pass

    # 5. Ingestion summary and logs
    ingest_log_path = root / "logs" / "ingestion_log.jsonl"
    ingestion_logs: list[dict[str, Any]] = []
    total_bytes = 0
    total_records = 0
    by_source: dict[str, int] = {}

    if ingest_log_path.exists():
        with open(ingest_log_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        entry = json.loads(line_str)
                        ingestion_logs.append(entry)
                        total_bytes += int(entry.get("bytes_written") or 0)
                        total_records += int(entry.get("record_count") or 0)
                        src = entry.get("source", "unknown")
                        by_source[src] = by_source.get(src, 0) + 1
                    except Exception:
                        pass

    ingestion_summary = {
        "total_logs": len(ingestion_logs),
        "total_bytes": total_bytes,
        "total_records": total_records,
        "by_source": by_source,
        "recent_logs": ingestion_logs[-10:] if ingestion_logs else [],
    }

    # 6. HDFS sync logs
    hdfs_log_path = root / "logs" / "hdfs_sync_log.jsonl"
    hdfs_logs: list[dict[str, Any]] = []
    if hdfs_log_path.exists():
        with open(hdfs_log_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        hdfs_logs.append(json.loads(line_str))
                    except Exception:
                        pass

    hdfs_summary = {
        "total_syncs": len(hdfs_logs),
        "logs": hdfs_logs,
    }

    return {
        "reconciliation": reconciliation,
        "quarantine_records": quarantine_records,
        "synthetic_quarantine_preview": synthetic_preview,
        "binary_file_errors": binary_file_errors,
        "ingestion_summary": ingestion_summary,
        "raw_ingestion_logs": ingestion_logs,
        "hdfs_summary": hdfs_summary,
    }
