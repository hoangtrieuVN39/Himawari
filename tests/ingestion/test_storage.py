import json
from pathlib import Path
import pytest
from src.ingestion.storage import save_raw_envelope, append_ingestion_log

def test_save_raw_envelope_atomic(tmp_path):
    base_dir = tmp_path / "raw"
    envelope = {
        "metadata": {
            "batch_id": "batch_001",
            "source": "metar",
            "fetched_at_utc": "2026-11-09T08:00:00Z"
        },
        "payload": [{"icaoId": "VVDN", "rawOb": "METAR VVDN..."}]
    }

    target_path = save_raw_envelope(
        source="metar",
        date_str="2026-11-09",
        filename="metar_batch_001.json",
        envelope=envelope,
        base_dir=str(base_dir)
    )

    saved_file = Path(target_path)
    assert saved_file.exists()
    assert saved_file.parent.name == "date=2026-11-09"
    assert saved_file.parent.parent.name == "metar"

    # Verify atomic tmp file is cleaned up
    tmp_file = saved_file.with_suffix(".json.tmp")
    assert not tmp_file.exists()

    # Verify content
    loaded = json.loads(saved_file.read_text(encoding="utf-8"))
    assert loaded["metadata"]["batch_id"] == "batch_001"
    assert len(loaded["payload"]) == 1

def test_append_ingestion_log(tmp_path):
    log_file = tmp_path / "logs" / "ingestion_log.jsonl"
    entry1 = {
        "batch_id": "batch_001",
        "source": "metar",
        "status": "SUCCESS",
        "bytes_written": 128
    }
    entry2 = {
        "batch_id": "batch_002",
        "source": "open_meteo",
        "status": "FAILED",
        "error_message": "Timeout"
    }

    append_ingestion_log(entry1, log_path=str(log_file))
    append_ingestion_log(entry2, log_path=str(log_file))

    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    row1 = json.loads(lines[0])
    row2 = json.loads(lines[1])
    assert row1["batch_id"] == "batch_001"
    assert row2["status"] == "FAILED"
