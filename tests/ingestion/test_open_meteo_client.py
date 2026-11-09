import json
from pathlib import Path
from unittest.mock import Mock
import pytest
from src.ingestion.open_meteo_client import ingest_open_meteo

def test_ingest_open_meteo_success(tmp_path, mocker):
    mock_fetch = mocker.patch("src.ingestion.open_meteo_client.fetch_with_retry")
    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.url = "https://api.open-meteo.com/v1/forecast?latitude=16.0439&longitude=108.1994..."
    mock_data = {
        "latitude": 16.0439,
        "longitude": 108.1994,
        "hourly": {
            "time": ["2026-11-09T08:00", "2026-11-09T09:00"],
            "precipitation": [0.0, 1.2],
            "rain": [0.0, 1.0]
        }
    }
    mock_resp.json.return_value = mock_data
    mock_resp.content = json.dumps(mock_data).encode("utf-8")
    mock_fetch.return_value = mock_resp

    airports_cfg = {
        "VVDN": {"icao": "VVDN", "name": "Da Nang", "latitude": 16.0439, "longitude": 108.1994}
    }

    results = ingest_open_meteo(
        airports_config=airports_cfg,
        expected_slot_utc="2026-11-09T08:00:00Z",
        base_dir=str(tmp_path)
    )

    assert len(results) == 1
    res = results[0]
    assert res["status"] == "SUCCESS"
    assert res["airport"] == "VVDN"
    assert res["http_status"] == 200
    assert res["file_path"] is not None

    saved_file = Path(res["file_path"])
    assert saved_file.exists()
    envelope = json.loads(saved_file.read_text(encoding="utf-8"))
    assert envelope["metadata"]["source"] == "open_meteo_forecast"
    assert envelope["metadata"]["airport_icao"] == "VVDN"
    assert envelope["metadata"]["forecast_hours_count"] == 2

    # Check log file
    log_file = tmp_path / "logs" / "ingestion_log.jsonl"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").strip().split("\n")
    log_entry = json.loads(lines[-1])
    assert log_entry["source"] == "open_meteo"
    assert log_entry["airport"] == "VVDN"
    assert log_entry["status"] == "SUCCESS"

def test_ingest_open_meteo_failure(tmp_path, mocker):
    mock_fetch = mocker.patch("src.ingestion.open_meteo_client.fetch_with_retry")
    mock_fetch.side_effect = Exception("Open-Meteo Rate Limit")

    airports_cfg = {
        "VVNB": {"icao": "VVNB", "name": "Noi Bai", "latitude": 21.2212, "longitude": 105.8072}
    }

    results = ingest_open_meteo(
        airports_config=airports_cfg,
        expected_slot_utc="2026-11-09T08:00:00Z",
        base_dir=str(tmp_path)
    )

    assert len(results) == 1
    res = results[0]
    assert res["status"] == "FAILED"
    assert res["airport"] == "VVNB"
    assert "Rate Limit" in res["error_message"]

    log_file = tmp_path / "logs" / "ingestion_log.jsonl"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").strip().split("\n")
    log_entry = json.loads(lines[-1])
    assert log_entry["status"] == "FAILED"
    assert log_entry["error_type"] == "Exception"
