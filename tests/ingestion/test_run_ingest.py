import pytest
from unittest.mock import Mock
from src.ingestion.run_ingest import parse_args, run_cycle

def test_parse_args_defaults():
    args = parse_args([])
    assert args.once is False
    assert args.loop is False
    assert args.backfill_hours == 2
    assert args.config == "config/airports.json"
    assert args.base_dir == "data"

def test_parse_args_custom():
    args = parse_args(["--once", "--backfill-hours", "24", "--config", "my_airports.json", "--base-dir", "custom_data"])
    assert args.once is True
    assert args.backfill_hours == 24
    assert args.config == "my_airports.json"
    assert args.base_dir == "custom_data"

def test_run_cycle_dispatches_both(mocker):
    mock_load_cfg = mocker.patch("src.ingestion.run_ingest.load_airports_config")
    mock_ingest_metar = mocker.patch("src.ingestion.run_ingest.ingest_metar")
    mock_ingest_forecast = mocker.patch("src.ingestion.run_ingest.ingest_open_meteo")

    mock_load_cfg.return_value = {
        "VVDN": {"icao": "VVDN", "name": "Da Nang", "latitude": 16.0, "longitude": 108.0},
        "VVNB": {"icao": "VVNB", "name": "Noi Bai", "latitude": 21.0, "longitude": 105.0}
    }
    mock_ingest_metar.return_value = {"status": "SUCCESS", "record_count": 4}
    mock_ingest_forecast.return_value = [
        {"airport": "VVDN", "status": "SUCCESS"},
        {"airport": "VVNB", "status": "SUCCESS"}
    ]

    summary = run_cycle(
        config_path="dummy.json",
        base_dir="dummy_data",
        backfill_hours=24,
        is_backfill=True,
        fetch_forecast=True
    )

    assert summary["metar"]["status"] == "SUCCESS"
    assert len(summary["open_meteo"]) == 2
    mock_ingest_metar.assert_called_once_with(
        airports=["VVDN", "VVNB"],
        hours=24,
        is_backfill=True,
        base_dir="dummy_data"
    )
    mock_ingest_forecast.assert_called_once()
