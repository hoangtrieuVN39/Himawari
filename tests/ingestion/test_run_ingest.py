from datetime import datetime, timezone
from unittest.mock import Mock, patch
import pytest

from src.ingestion.run_ingest import (
    parse_args,
    run_cycle,
    run_himawari_slot_pipeline,
    run_himawari_window_backfill,
    main,
)


def test_parse_args_defaults():
    args = parse_args([])
    assert args.once is False
    assert args.loop is False
    assert args.backfill_hours == 2
    assert args.config == "config/airports.json"
    assert args.base_dir == "data"
    assert args.include_himawari is False
    assert args.himawari_window is None
    assert args.sync_hdfs is False
    assert args.inject_corrupt_test is False
    assert args.retain_raw_hours == 24


def test_parse_args_custom():
    args = parse_args([
        "--once",
        "--backfill-hours", "24",
        "--config", "my_airports.json",
        "--base-dir", "custom_data",
    ])
    assert args.once is True
    assert args.backfill_hours == 24
    assert args.config == "my_airports.json"
    assert args.base_dir == "custom_data"


def test_parse_args_module2_flags():
    args = parse_args([
        "--once",
        "--include-himawari",
        "--sync-hdfs",
        "--inject-corrupt-test",
        "--himawari-window", "2026-11-09T06:00:00Z", "2026-11-09T08:30:00Z",
        "--retain-raw-hours", "48",
    ])
    assert args.include_himawari is True
    assert args.sync_hdfs is True
    assert args.inject_corrupt_test is True
    assert args.himawari_window == ["2026-11-09T06:00:00Z", "2026-11-09T08:30:00Z"]
    assert args.retain_raw_hours == 48


def test_run_cycle_dispatches_both(mocker):
    mock_load_cfg = mocker.patch("src.ingestion.run_ingest.load_airports_config")
    mock_ingest_metar = mocker.patch("src.ingestion.run_ingest.ingest_metar")
    mock_ingest_forecast = mocker.patch("src.ingestion.run_ingest.ingest_open_meteo")

    mock_load_cfg.return_value = {
        "VVDN": {"icao": "VVDN", "name": "Da Nang", "latitude": 16.0, "longitude": 108.0},
        "VVNB": {"icao": "VVNB", "name": "Noi Bai", "latitude": 21.0, "longitude": 105.0},
    }
    mock_ingest_metar.return_value = {"status": "SUCCESS", "record_count": 4}
    mock_ingest_forecast.return_value = [
        {"airport": "VVDN", "status": "SUCCESS"},
        {"airport": "VVNB", "status": "SUCCESS"},
    ]

    summary = run_cycle(
        config_path="dummy.json",
        base_dir="dummy_data",
        backfill_hours=24,
        is_backfill=True,
        fetch_forecast=True,
    )

    assert summary["metar"]["status"] == "SUCCESS"
    assert len(summary["open_meteo"]) == 2
    mock_ingest_metar.assert_called_once_with(
        airports=["VVDN", "VVNB"],
        hours=24,
        is_backfill=True,
        base_dir="dummy_data",
    )
    mock_ingest_forecast.assert_called_once()


def test_run_cycle_with_himawari(mocker):
    mocker.patch("src.ingestion.run_ingest.load_airports_config", return_value={"VVDN": {"icao": "VVDN"}})
    mocker.patch("src.ingestion.run_ingest.ingest_metar", return_value={"status": "SUCCESS"})
    mocker.patch("src.ingestion.run_ingest.ingest_open_meteo", return_value=[])
    mock_find_slot = mocker.patch(
        "src.ingestion.run_ingest.find_latest_available_slot",
        return_value=datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc),
    )
    mock_pipeline = mocker.patch(
        "src.ingestion.run_ingest.run_himawari_slot_pipeline",
        return_value={"scan_slot_utc": "2026-11-09T08:00:00Z", "status": "SUCCESS"},
    )

    summary = run_cycle(
        config_path="dummy.json",
        base_dir="dummy_data",
        include_himawari=True,
    )

    assert mock_find_slot.called
    assert mock_pipeline.called
    assert summary["himawari"]["status"] == "SUCCESS"


def test_run_himawari_slot_pipeline(mocker):
    mock_dl = mocker.patch(
        "src.ingestion.run_ingest.download_himawari_slot",
        return_value={"status": "SUCCESS", "files": {"S0410": "dummy_path"}},
    )
    mock_proc = mocker.patch(
        "src.ingestion.run_ingest.process_himawari_slot",
        return_value={"metadata": {"batch_id": "test_batch"}},
    )
    mock_clean = mocker.patch(
        "src.ingestion.run_ingest.cleanup_old_raw_segments",
        return_value=2,
    )

    airports_cfg = {
        "VVDN": {"icao": "VVDN", "himawari_segment": "S0410"},
        "VVNB": {"icao": "VVNB", "himawari_segment": "S0410"},
        "VVTS": {"icao": "VVTS", "himawari_segment": "S0510"},
    }
    scan_dt = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)

    res = run_himawari_slot_pipeline(
        scan_dt_utc=scan_dt,
        airports_cfg=airports_cfg,
        base_dir="data_test",
        is_backfill=False,
        retain_raw_hours=24,
    )

    mock_dl.assert_called_once_with(
        scan_dt,
        segments=["S0410", "S0510"],
        base_dir="data_test",
        is_backfill=False,
    )
    mock_proc.assert_called_once()
    mock_clean.assert_called_once_with(
        base_dir="data_test",
        retain_hours=24,
        pinned_slots=None,
    )
    assert res["scan_slot_utc"] == "2026-11-09T08:00:00Z"
    assert res["deleted_segments"] == 2


def test_run_himawari_slot_pipeline_archive_fallback(mocker):
    # First download fails (e.g. 2026 slot not on S3)
    mock_dl = mocker.patch(
        "src.ingestion.run_ingest.download_himawari_slot",
        side_effect=[
            {"status": "FAILED", "files": {}},
            {"status": "SUCCESS", "files": {"S0410": "resolved_s3_path"}, "source_slot_utc": "2025-01-01T08:00:00Z"},
        ],
    )
    mock_find_latest = mocker.patch(
        "src.ingestion.run_ingest.find_latest_available_slot",
        return_value=datetime(2025, 1, 1, 3, 0, tzinfo=timezone.utc),
    )
    mock_proc = mocker.patch(
        "src.ingestion.run_ingest.process_himawari_slot",
        return_value={"metadata": {}},
    )
    mocker.patch("src.ingestion.run_ingest.cleanup_old_raw_segments", return_value=0)

    airports_cfg = {"VVDN": {"icao": "VVDN", "himawari_segment": "S0410"}}
    scan_dt = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)

    res = run_himawari_slot_pipeline(
        scan_dt_utc=scan_dt,
        airports_cfg=airports_cfg,
        base_dir="data_test",
    )

    assert mock_find_latest.called
    assert mock_dl.call_count == 2
    # The substituted scan keeps its real time; it must not be relabelled as the requested slot
    mock_proc.assert_called_once()
    assert mock_proc.call_args.kwargs["scan_slot_utc"] == "2025-01-01T08:00:00Z"


def test_run_himawari_window_backfill(mocker):
    mock_pipeline = mocker.patch(
        "src.ingestion.run_ingest.run_himawari_slot_pipeline",
        return_value={"status": "SUCCESS"},
    )
    mocker.patch(
        "src.ingestion.run_ingest.load_airports_config",
        return_value={"VVDN": {"icao": "VVDN"}},
    )

    # 30-minute window -> 4 slots: 08:00, 08:10, 08:20, 08:30
    results = run_himawari_window_backfill(
        start_iso="2026-11-09T08:00:00Z",
        end_iso="2026-11-09T08:30:00Z",
        config_path="dummy.json",
        base_dir="test_data",
        retain_raw_hours=24,
    )

    assert len(results) == 4
    assert mock_pipeline.call_count == 4
    # Check that pinned_slots contains all 4 slots
    last_call = mock_pipeline.call_args_list[-1]
    pinned = last_call.kwargs["pinned_slots"]
    assert len(pinned) == 4
    assert "2026-11-09T08:00:00Z" in pinned
    assert "2026-11-09T08:30:00Z" in pinned


def test_main_cli_dispatch_inject_corrupt_test(mocker):
    mocker.patch("sys.argv", ["run_ingest.py", "--inject-corrupt-test"])
    mock_inject = mocker.patch(
        "src.ingestion.run_ingest.inject_synthetic_corrupt_test",
        return_value={"is_synthetic_test": True},
    )

    main()
    mock_inject.assert_called_once_with(base_dir="data")


def test_main_cli_dispatch_himawari_window(mocker):
    mocker.patch(
        "sys.argv",
        [
            "run_ingest.py",
            "--himawari-window", "2026-11-09T08:00:00Z", "2026-11-09T08:20:00Z",
            "--sync-hdfs",
        ],
    )
    mock_backfill = mocker.patch(
        "src.ingestion.run_ingest.run_himawari_window_backfill",
        return_value=[{"status": "SUCCESS"}, {"status": "SUCCESS"}, {"status": "SUCCESS"}],
    )
    mock_hdfs = mocker.patch("src.ingestion.run_ingest.WebHDFSClient")

    main()
    mock_backfill.assert_called_once_with(
        start_iso="2026-11-09T08:00:00Z",
        end_iso="2026-11-09T08:20:00Z",
        config_path="config/airports.json",
        base_dir="data",
        retain_raw_hours=24,
    )
    assert mock_hdfs.return_value.sync_directory.called
