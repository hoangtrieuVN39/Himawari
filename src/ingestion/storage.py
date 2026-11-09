import json
import os
from pathlib import Path
from typing import Dict, Any

def save_raw_envelope(
    source: str,
    date_str: str,
    filename: str,
    envelope: Dict[str, Any],
    base_dir: str = "data/raw"
) -> str:
    """
    Saves raw envelope dictionary as JSON into a Hive-partitioned directory:
    <base_dir>/<source>/date=<date_str>/<filename>
    Uses atomic write via temporary file (.tmp) and os.replace to prevent corruption.
    Returns the absolute or relative path string of the saved file.
    """
    target_dir = Path(base_dir) / source / f"date={date_str}"
    target_dir.mkdir(parents=True, exist_ok=True)

    target_file = target_dir / filename
    temp_file = target_dir / f"{filename}.tmp"

    serialized = json.dumps(envelope, ensure_ascii=False, indent=2)

    with temp_file.open("w", encoding="utf-8") as f:
        f.write(serialized)
        f.flush()
        os.fsync(f.fileno())

    os.replace(temp_file, target_file)
    return str(target_file)

def append_ingestion_log(
    log_entry: Dict[str, Any],
    log_path: str = "data/logs/ingestion_log.jsonl"
) -> None:
    """
    Appends a single JSON log entry to the JSON Lines audit log file.
    Ensures parent directory exists and flushes disk write immediately.
    """
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    line = json.dumps(log_entry, ensure_ascii=False) + "\n"
    with path.open("a", encoding="utf-8") as f:
        f.write(line)
        f.flush()
