"""Tests for live data synchronization, CLI contracts, and late arrival handling."""

import sys
from pathlib import Path
from unittest import mock
import pytest

from src.dashboard.data_loader import fetch_and_update_realtime
from src.ingestion.run_ingest import parse_args


@pytest.mark.parametrize("include_himawari", [True, False])
def test_realtime_refresh_uses_ingestor_cli_contract(tmp_path: Path, include_himawari: bool):
    custom_dir = tmp_path / "Kho Dữ Liệu Thời Tiết 2026"
    custom_dir.mkdir(parents=True, exist_ok=True)

    captured_cmds = []

    def fake_subprocess_run(cmd, *args, **kwargs):
        captured_cmds.append(cmd)
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "OK"
        mock_proc.stderr = ""
        return mock_proc

    with mock.patch("subprocess.run", side_effect=fake_subprocess_run), \
         mock.patch("src.dashboard.data_loader.sync_latest_raw_to_decision_table", return_value=["2026-11-10T08:00:00Z"]), \
         mock.patch("src.dashboard.data_loader.load_decision_slots", return_value={"2026-11-10T08:00:00Z": []}):

        result = fetch_and_update_realtime(
            storage_root=str(custom_dir),
            include_himawari=include_himawari,
        )

        assert result["status"] == "SUCCESS"
        assert len(captured_cmds) == 1
        cmd = captured_cmds[0]

        assert cmd[0] == sys.executable
        assert cmd[1:3] == ["-m", "src.ingestion.run_ingest"]

        # Parse args through run_ingest's parser to verify exact contract
        parsed = parse_args(cmd[3:])
        assert parsed.base_dir == str(custom_dir)
        assert parsed.once is True
        assert getattr(parsed, "include_himawari", False) is include_himawari


def test_realtime_refresh_handles_subprocess_error():
    mock_proc = mock.MagicMock()
    mock_proc.returncode = 1
    mock_proc.stdout = ""
    mock_proc.stderr = "Network connection timeout to AviationWeather"

    with mock.patch("subprocess.run", return_value=mock_proc), \
         mock.patch("src.dashboard.data_loader.sync_latest_raw_to_decision_table") as mock_sync:

        result = fetch_and_update_realtime(storage_root="data")

        assert result["status"] == "ERROR"
        assert "Network connection timeout" in result["error"]
        assert not mock_sync.called


import json
from datetime import datetime, timezone
from src.dashboard.data_loader import load_decision_slots, sync_latest_raw_to_decision_table


def _write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def _setup_test_env(base_dir: Path):
    dec_path = base_dir / "processed" / "decision_table" / "decision_table.jsonl"
    dec_path.parent.mkdir(parents=True, exist_ok=True)
    dec_path.touch()
    return base_dir


def test_existing_live_slot_refreshes_after_late_forecast(tmp_path: Path):
    base_dir = _setup_test_env(tmp_path / "data")
    metar_file = base_dir / "raw" / "metar" / "date=2026-11-10" / "metar_0801.json"
    _write_json(metar_file, {
        "metadata": {
            "batch_id": "metar_0801",
            "source": "aviationweather_metar",
            "fetched_at_utc": "2026-11-10T08:01:00Z",
            "is_backfill": False,
        },
        "payload": [
            {
                "icaoId": ap,
                "obsTime": int(datetime(2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc).timestamp()),
                "reportTime": "2026-11-10T08:00:00Z",
                "rawOb": f"METAR {ap} 100800Z 09010KT 9999 FEW020 28/24 Q1012",
                "visib": "10+",
                "wxString": None,
                "temp": 28.0,
                "dewp": 24.0,
            }
            for ap in ["VVDN", "VVNB", "VVTS"]
        ]
    })

    # Initial load: slot 08:05Z created, VVDN Level 4 (Routine)
    sync_latest_raw_to_decision_table(str(base_dir))
    slots = load_decision_slots(str(base_dir))
    assert "2026-11-10T08:05:00Z" in slots
    slot_0805 = slots["2026-11-10T08:05:00Z"]
    assert len(slot_0805) == 3
    vvdn_row = next(r for r in slot_0805 if r["airport_icao"] == "VVDN")
    assert vvdn_row["priority_level"] == 4
    assert vvdn_row.get("forecast_rain_next_hour_mm") is None

    # Now a late forecast arrives (fetched at 08:02Z <= slot 08:05Z, valid 09:00Z, rain 5.0mm)
    fc_file = base_dir / "raw" / "open_meteo" / "date=2026-11-10" / "fc_VVDN_0802.json"
    _write_json(fc_file, {
        "metadata": {
            "batch_id": "fc_0802",
            "source": "open_meteo_forecast",
            "airport_icao": "VVDN",
            "fetched_at_utc": "2026-11-10T08:02:00Z",
        },
        "payload": {
            "hourly": {
                "time": ["2026-11-10T09:00:00Z"],
                "precipitation": [5.0],
                "rain": [5.0],
                "showers": [0.0],
                "weather_code": [61],
                "visibility": [10000.0],
            }
        }
    })

    # Next load must refresh existing live slot 08:05Z without adding duplicate rows
    sync_latest_raw_to_decision_table(str(base_dir))
    slots_after = load_decision_slots(str(base_dir))
    slot_0805_after = slots_after["2026-11-10T08:05:00Z"]
    assert len(slot_0805_after) == 3
    vvdn_after = next(r for r in slot_0805_after if r["airport_icao"] == "VVDN")
    assert vvdn_after["forecast_rain_next_hour_mm"] == 5.0
    assert vvdn_after["priority_level"] == 3


def test_older_live_slot_refreshes_after_late_input(tmp_path: Path):
    base_dir = _setup_test_env(tmp_path / "data")
    # Slot 1: fetched 08:01Z -> slot 08:05Z
    _write_json(base_dir / "raw" / "metar" / "date=2026-11-10" / "metar_0801.json", {
        "metadata": {
            "batch_id": "metar_0801",
            "source": "aviationweather_metar",
            "fetched_at_utc": "2026-11-10T08:01:00Z",
            "is_backfill": False,
        },
        "payload": [
            {
                "icaoId": ap,
                "obsTime": int(datetime(2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc).timestamp()),
                "reportTime": "2026-11-10T08:00:00Z",
                "rawOb": f"METAR {ap} 100800Z 09010KT 9999 FEW020 28/24 Q1012",
                "visib": "10+",
                "wxString": None,
                "temp": 28.0,
                "dewp": 24.0,
            }
            for ap in ["VVDN", "VVNB", "VVTS"]
        ]
    })
    # Slot 2: fetched 08:11Z -> slot 08:15Z
    _write_json(base_dir / "raw" / "metar" / "date=2026-11-10" / "metar_0811.json", {
        "metadata": {
            "batch_id": "metar_0811",
            "source": "aviationweather_metar",
            "fetched_at_utc": "2026-11-10T08:11:00Z",
            "is_backfill": False,
        },
        "payload": [
            {
                "icaoId": ap,
                "obsTime": int(datetime(2026, 11, 10, 8, 10, 0, tzinfo=timezone.utc).timestamp()),
                "reportTime": "2026-11-10T08:10:00Z",
                "rawOb": f"METAR {ap} 100810Z 09010KT 9999 FEW020 28/24 Q1012",
                "visib": "10+",
                "wxString": None,
                "temp": 28.0,
                "dewp": 24.0,
            }
            for ap in ["VVDN", "VVNB", "VVTS"]
        ]
    })

    # Both slots are loaded; slot 08:05Z is older, 08:15Z is latest
    sync_latest_raw_to_decision_table(str(base_dir))
    slots = load_decision_slots(str(base_dir))
    assert "2026-11-10T08:05:00Z" in slots
    assert "2026-11-10T08:15:00Z" in slots

    # Add late forecast for VVDN fetched at 08:02Z (valid for both 08:05Z and 08:15Z)
    _write_json(base_dir / "raw" / "open_meteo" / "date=2026-11-10" / "fc_VVDN_0802.json", {
        "metadata": {
            "batch_id": "fc_0802",
            "source": "open_meteo_forecast",
            "airport_icao": "VVDN",
            "fetched_at_utc": "2026-11-10T08:02:00Z",
        },
        "payload": {
            "hourly": {
                "time": ["2026-11-10T09:00:00Z"],
                "precipitation": [5.0],
                "rain": [5.0],
                "showers": [0.0],
                "weather_code": [61],
                "visibility": [10000.0],
            }
        }
    })

    # Add forecast for VVNB fetched at 08:06Z (only valid for 08:15Z, NOT for 08:05Z!)
    _write_json(base_dir / "raw" / "open_meteo" / "date=2026-11-10" / "fc_VVNB_0806.json", {
        "metadata": {
            "batch_id": "fc_0806",
            "source": "open_meteo_forecast",
            "airport_icao": "VVNB",
            "fetched_at_utc": "2026-11-10T08:06:00Z",
        },
        "payload": {
            "hourly": {
                "time": ["2026-11-10T09:00:00Z"],
                "precipitation": [7.0],
                "rain": [7.0],
                "showers": [0.0],
                "weather_code": [61],
                "visibility": [10000.0],
            }
        }
    })

    sync_latest_raw_to_decision_table(str(base_dir))

    slots_after = load_decision_slots(str(base_dir))
    slot_0805 = slots_after["2026-11-10T08:05:00Z"]
    vvdn_0805 = next(r for r in slot_0805 if r["airport_icao"] == "VVDN")
    vvnb_0805 = next(r for r in slot_0805 if r["airport_icao"] == "VVNB")

    # Older slot 08:05Z MUST be refreshed with VVDN rain 5.0
    assert vvdn_0805["forecast_rain_next_hour_mm"] == 5.0
    assert vvdn_0805["priority_level"] == 3

    # VVNB forecast fetched at 08:06Z must NOT leak into slot 08:05Z!
    assert vvnb_0805.get("forecast_rain_next_hour_mm") is None

    # Slot 08:15Z must have VVNB rain 7.0
    slot_0815 = slots_after["2026-11-10T08:15:00Z"]
    vvnb_0815 = next(r for r in slot_0815 if r["airport_icao"] == "VVNB")
    assert vvnb_0815["forecast_rain_next_hour_mm"] == 7.0


def test_signature_cache_skips_recomputation_when_unchanged(tmp_path: Path):
    base_dir = _setup_test_env(tmp_path / "data")
    metar_file = base_dir / "raw" / "metar" / "date=2026-11-10" / "metar_0801.json"
    _write_json(metar_file, {
        "metadata": {
            "batch_id": "metar_0801",
            "source": "aviationweather_metar",
            "fetched_at_utc": "2026-11-10T08:01:00Z",
            "is_backfill": False,
        },
        "payload": [
            {
                "icaoId": ap,
                "obsTime": int(datetime(2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc).timestamp()),
                "reportTime": "2026-11-10T08:00:00Z",
                "rawOb": f"METAR {ap} 100800Z 09010KT 9999 FEW020 28/24 Q1012",
                "visib": "10+",
                "wxString": None,
                "temp": 28.0,
                "dewp": 24.0,
            }
            for ap in ["VVDN", "VVNB", "VVTS"]
        ]
    })

    # Initial sync computes slot
    res1 = sync_latest_raw_to_decision_table(str(base_dir))
    assert res1 == ["2026-11-10T08:05:00Z"]

    # Second sync without changes must hit cache and return [] without recomputing
    with mock.patch("src.dashboard.data_loader._compute_slot_rows_from_raw") as mock_compute:
        res2 = sync_latest_raw_to_decision_table(str(base_dir))
        assert res2 == []
        assert not mock_compute.called

    # force_latest=True must bypass cache and recompute
    with mock.patch("src.dashboard.data_loader._compute_slot_rows_from_raw", return_value=[
        {"airport_icao": ap, "priority_level": 4} for ap in ["VVDN", "VVNB", "VVTS"]
    ]) as mock_compute:
        res3 = sync_latest_raw_to_decision_table(str(base_dir), force_latest=True)
        assert res3 == ["2026-11-10T08:05:00Z"]
        assert mock_compute.called


def test_modifying_same_json_file_invalidates_cache(tmp_path: Path):
    base_dir = _setup_test_env(tmp_path / "data")
    metar_file = base_dir / "raw" / "metar" / "date=2026-11-10" / "metar_0801.json"
    _write_json(metar_file, {
        "metadata": {
            "batch_id": "metar_0801",
            "source": "aviationweather_metar",
            "fetched_at_utc": "2026-11-10T08:01:00Z",
            "is_backfill": False,
        },
        "payload": [
            {
                "icaoId": ap,
                "obsTime": int(datetime(2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc).timestamp()),
                "reportTime": "2026-11-10T08:00:00Z",
                "rawOb": f"METAR {ap} 100800Z 09010KT 9999 FEW020 28/24 Q1012",
                "visib": "10+",
                "wxString": None,
                "temp": 28.0,
                "dewp": 24.0,
            }
            for ap in ["VVDN", "VVNB", "VVTS"]
        ]
    })
    sync_latest_raw_to_decision_table(str(base_dir))

    # Modify file content (e.g. weather becomes -RA)
    _write_json(metar_file, {
        "metadata": {
            "batch_id": "metar_0801",
            "source": "aviationweather_metar",
            "fetched_at_utc": "2026-11-10T08:01:00Z",
            "is_backfill": False,
        },
        "payload": [
            {
                "icaoId": ap,
                "obsTime": int(datetime(2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc).timestamp()),
                "reportTime": "2026-11-10T08:00:00Z",
                "rawOb": f"METAR {ap} 100800Z 09010KT 8000 -RA FEW020 28/24 Q1012",
                "visib": "8000",
                "wxString": "-RA",
                "temp": 28.0,
                "dewp": 24.0,
            }
            for ap in ["VVDN", "VVNB", "VVTS"]
        ]
    })

    # Cache must detect mtime/size change and recompute
    sync_latest_raw_to_decision_table(str(base_dir))
    slots = load_decision_slots(str(base_dir))
    vvdn = next(r for r in slots["2026-11-10T08:05:00Z"] if r["airport_icao"] == "VVDN")
    assert vvdn["priority_level"] == 1
    assert "mưa/dông tại trạm" in vvdn["reason_vi"]


def test_spark_table_overwrite_invalidates_cache(tmp_path: Path):
    base_dir = _setup_test_env(tmp_path / "data")
    metar_file = base_dir / "raw" / "metar" / "date=2026-11-10" / "metar_0801.json"
    _write_json(metar_file, {
        "metadata": {
            "batch_id": "metar_0801",
            "source": "aviationweather_metar",
            "fetched_at_utc": "2026-11-10T08:01:00Z",
            "is_backfill": False,
        },
        "payload": [
            {
                "icaoId": ap,
                "obsTime": int(datetime(2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc).timestamp()),
                "reportTime": "2026-11-10T08:00:00Z",
                "rawOb": f"METAR {ap} 100800Z 09010KT 9999 FEW020 28/24 Q1012",
                "visib": "10+",
                "wxString": None,
                "temp": 28.0,
                "dewp": 24.0,
            }
            for ap in ["VVDN", "VVNB", "VVTS"]
        ]
    })
    sync_latest_raw_to_decision_table(str(base_dir))

    # Overwrite decision_table.jsonl with simulated Spark output
    dec_path = base_dir / "processed" / "decision_table" / "decision_table.jsonl"
    with dec_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "review_slot_utc": "2026-11-09T07:00:00Z",
            "airport_icao": "VVDN",
            "priority_level": 1,
            "reason_vi": "Spark historical output",
        }) + "\n")

    # Sync must detect decision table changed, recompute live slots while preserving Spark row
    sync_latest_raw_to_decision_table(str(base_dir))
    slots = load_decision_slots(str(base_dir))
    assert "2026-11-09T07:00:00Z" in slots
    assert "2026-11-10T08:05:00Z" in slots





def _write_basic_metar(base_dir: Path, batch: str = "metar_0801", fetched: str = "2026-11-10T08:01:00Z"):
    _write_json(base_dir / "raw" / "metar" / "date=2026-11-10" / f"{batch}.json", {
        "metadata": {"batch_id": batch, "source": "aviationweather_metar",
                     "fetched_at_utc": fetched, "is_backfill": False},
        "payload": [
            {"icaoId": ap, "obsTime": int(datetime(2026, 11, 10, 8, 0, 0, tzinfo=timezone.utc).timestamp()),
             "reportTime": "2026-11-10T08:00:00Z",
             "rawOb": f"METAR {ap} 100800Z 09010KT 9999 FEW020 28/24 Q1012",
             "visib": "10+", "wxString": None, "temp": 28.0, "dewp": 24.0}
            for ap in ["VVDN", "VVNB", "VVTS"]
        ],
    })


def test_load_decision_slots_is_read_only(tmp_path: Path):
    """Reading must never rewrite decision_table.jsonl, even when newer raw batches exist."""
    base_dir = _setup_test_env(tmp_path / "data")
    _write_basic_metar(base_dir)
    dec_path = base_dir / "processed" / "decision_table" / "decision_table.jsonl"
    before = (dec_path.read_bytes(), dec_path.stat().st_mtime_ns)

    load_decision_slots(str(base_dir))

    assert (dec_path.read_bytes(), dec_path.stat().st_mtime_ns) == before


def test_sync_cache_is_isolated_per_storage_root(tmp_path: Path):
    root_a = _setup_test_env(tmp_path / "a")
    root_b = _setup_test_env(tmp_path / "b")
    _write_basic_metar(root_a)
    _write_basic_metar(root_b)

    assert sync_latest_raw_to_decision_table(str(root_a)) == ["2026-11-10T08:05:00Z"]
    # Root B has identical relative content but must not be skipped because of A's cache entry
    assert sync_latest_raw_to_decision_table(str(root_b)) == ["2026-11-10T08:05:00Z"]


def test_concurrent_syncs_leave_valid_table_and_no_temp_files(tmp_path: Path):
    import threading

    base_dir = _setup_test_env(tmp_path / "data")
    _write_basic_metar(base_dir)
    errors: list[Exception] = []

    def worker():
        try:
            sync_latest_raw_to_decision_table(str(base_dir), force_latest=True)
        except Exception as exc:  # pragma: no cover - failure path asserted below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=60)

    assert not errors
    dec_dir = base_dir / "processed" / "decision_table"
    rows = [json.loads(l) for l in (dec_dir / "decision_table.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(rows) == 3
    assert [p.name for p in dec_dir.iterdir()] == ["decision_table.jsonl"]


def test_fetch_and_update_realtime_handles_ingestion_timeout():
    import subprocess

    with mock.patch(
        "src.dashboard.data_loader.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="ingest", timeout=180),
    ), mock.patch("src.dashboard.data_loader.sync_latest_raw_to_decision_table") as mock_sync:
        result = fetch_and_update_realtime(storage_root="data")

    assert result["status"] == "ERROR"
    assert "timeout" in result["error"].lower()
    assert not mock_sync.called


def test_replay_load_is_fast(isolated_data):
    """Replay loading of the real decision table must stay interactive (< 1s)."""
    import time

    start = time.perf_counter()
    slots = load_decision_slots(str(isolated_data))
    elapsed = time.perf_counter() - start

    assert slots
    assert elapsed < 1.0, f"load_decision_slots took {elapsed:.2f}s"