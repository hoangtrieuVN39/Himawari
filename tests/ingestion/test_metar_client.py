import json
from pathlib import Path
from unittest.mock import Mock
import pytest
from src.ingestion.metar_client import ingest_metar

def test_ingest_metar_success(tmp_path, mocker):
    mock_fetch = mocker.patch("src.ingestion.metar_client.fetch_with_retry")
    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.url = "https://aviationweather.gov/api/data/metar?ids=VVDN%2CVVNB%2CVVTS&format=json&hours=2"
    mock_data = [
        {"icaoId": "VVDN", "reportTime": "2026-11-09 08:00:00", "rawOb": "METAR VVDN..."},
        {"icaoId": "VVNB", "reportTime": "2026-11-09 08:00:00", "rawOb": "METAR VVNB..."}
    ]
    mock_resp.json.return_value = mock_data
    mock_resp.content = json.dumps(mock_data).encode("utf-8")
    mock_fetch.return_value = mock_resp

    result = ingest_metar(
        airports=["VVDN", "VVNB", "VVTS"],
        hours=2,
        expected_slot_utc="2026-11-09T08:00:00Z",
        base_dir=str(tmp_path)
    )

    assert result["status"] == "SUCCESS"
    assert result["record_count"] == 2
    assert result["http_status"] == 200
    assert result["file_path"] is not None

    saved_file = Path(result["file_path"])
    assert saved_file.exists()
    envelope = json.loads(saved_file.read_text(encoding="utf-8"))
    assert envelope["metadata"]["source"] == "aviationweather_metar"
    assert envelope["metadata"]["record_count"] == 2
    assert len(envelope["payload"]) == 2

    # Check ingestion log
    log_file = tmp_path / "logs" / "ingestion_log.jsonl"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").strip().split("\n")
    log_entry = json.loads(lines[-1])
    assert log_entry["source"] == "metar"
    assert log_entry["status"] == "SUCCESS"
    assert log_entry["record_count"] == 2

def test_ingest_metar_failure(tmp_path, mocker):
    mock_fetch = mocker.patch("src.ingestion.metar_client.fetch_with_retry")
    mock_fetch.side_effect = Exception("DNS Resolution Error")

    result = ingest_metar(
        airports=["VVDN", "VVNB", "VVTS"],
        hours=2,
        expected_slot_utc="2026-11-09T08:00:00Z",
        base_dir=str(tmp_path)
    )

    assert result["status"] == "FAILED"
    assert result["record_count"] == 0
    assert "DNS Resolution Error" in result["error_message"]

    # Check ingestion log was recorded even on failure
    log_file = tmp_path / "logs" / "ingestion_log.jsonl"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").strip().split("\n")
    log_entry = json.loads(lines[-1])
    assert log_entry["status"] == "FAILED"
    assert log_entry["error_type"] == "Exception"

