"""Data loading, timestamp normalization, and audit parsing for Streamlit Dashboard."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import re
from typing import Any


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


def load_decision_slots(storage_root: str = "data") -> dict[str, list[dict[str, Any]]]:
    """Load decision table from decision_table.jsonl and group by normalized UTC slot.
    
    Returns:
        dict[utc_iso, list[row_dict]] where utc_iso is e.g. "2026-11-09T07:00:00Z"
    """
    path = Path(storage_root) / "processed" / "decision_table" / "decision_table.jsonl"
    if not path.exists():
        return {}

    slots: dict[str, list[dict[str, Any]]] = {}

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            try:
                row = json.loads(line_str)
            except Exception:
                continue

            slot_info = normalize_timestamp_pair(row.get("review_slot_utc"), row.get("raw_ob"))
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

            key = slot_info["utc_iso"]
            if key not in slots:
                slots[key] = []
            slots[key].append(row)

    # Ensure accepted_first_review_airports is present and consistent on all rows in each slot
    for slot_key, rows in slots.items():
        if rows:
            # If not present in raw row, compute from minimum priority_level
            min_priority = min(r.get("priority_level", 999) for r in rows)
            default_accepted = sorted([r["airport_icao"] for r in rows if r.get("priority_level") == min_priority])
            for r in rows:
                if not r.get("accepted_first_review_airports"):
                    r["accepted_first_review_airports"] = default_accepted

    return slots


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
) -> list[dict[str, str]]:
    """Scan and return up to max_frames B13 PNG images chronologically <= up_to_utc_iso.
    
    Prevents future data leakage by strictly checking scan_time <= up_to_utc_iso.
    
    Returns:
        list of dict: [{"scan_time_utc": str, "utc_hhmm": str, "vn_hhmm": str, "png_path": str}]
    """
    airport_dir = Path(storage_root) / "processed" / "himawari_png" / f"airport={airport_icao}"
    if not airport_dir.exists():
        return []

    norm_target = normalize_timestamp_pair(up_to_utc_iso)
    cutoff_dt = datetime.fromisoformat(norm_target["utc_iso"].replace("Z", "+00:00"))

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
        if dt_scan <= cutoff_dt:
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
    """Load reconciliation summary, quarantine records, synthetic quarantine, and operational logs."""
    root = Path(storage_root)

    # 1. Reconciliation summary
    recon_path = root / "processed" / "reconciliation" / "reconciliation_summary.json"
    reconciliation = {"all_sources_reconciled": False, "sources": []}
    if recon_path.exists():
        try:
            with open(recon_path, "r", encoding="utf-8") as f:
                reconciliation = json.load(f)
        except Exception:
            pass

    # 2. Quarantine records
    quarantine_path = root / "processed" / "quarantine" / "quarantine_records.jsonl"
    quarantine_records: list[dict[str, Any]] = []
    if quarantine_path.exists():
        with open(quarantine_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        quarantine_records.append(json.loads(line_str))
                    except Exception:
                        pass

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
