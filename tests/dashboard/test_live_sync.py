"""LIVE refresh contract: ingestion CLI, Spark publication of a new Gold version, read-only loading."""

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from src.dashboard import data_loader
from src.dashboard.data_loader import (
    fetch_and_update_realtime,
    has_new_decision_version,
    load_decision_slots,
)
from src.ingestion.run_ingest import parse_args
from src.spark.run_pipeline import parse_args as parse_pipeline_args
from tests.dashboard.lake_fixtures import decision_row, finalize_slot, write_lake_version

SLOT = "2026-11-10T08:05:00Z"


def _ok_proc():
    proc = mock.MagicMock()
    proc.returncode, proc.stdout, proc.stderr = 0, "OK", ""
    return proc


def _write_gold(root: Path, run_id: str, slot: str = SLOT) -> None:
    rows = finalize_slot([decision_row(slot, "VVDN", 1), decision_row(slot, "VVNB", 4), decision_row(slot, "VVTS", 4)])
    write_lake_version(root, "decision_table", rows, run_id=run_id)


@pytest.mark.parametrize("include_himawari", [True, False])
def test_realtime_refresh_ingests_then_publishes_with_spark(tmp_path: Path, include_himawari: bool):
    root = tmp_path / "Kho Dữ Liệu Thời Tiết 2026"
    root.mkdir()
    captured = []

    def fake_run(cmd, *args, **kwargs):
        captured.append(cmd)
        if cmd[2] == "src.spark.run_pipeline":
            _write_gold(root, "20261110T080600000000Z")
        return _ok_proc()

    with mock.patch("src.dashboard.data_loader.subprocess.run", side_effect=fake_run):
        result = fetch_and_update_realtime(storage_root=str(root), include_himawari=include_himawari)

    assert result["status"] == "SUCCESS"
    assert result["published_by"] == "pipeline"
    assert result["latest_slot_utc"] == SLOT
    ingest_cmd, spark_cmd = captured
    assert ingest_cmd[:3] == [sys.executable, "-m", "src.ingestion.run_ingest"]
    parsed = parse_args(ingest_cmd[3:])
    assert parsed.base_dir == str(root) and parsed.once is True
    assert parsed.include_himawari is include_himawari
    assert spark_cmd[:3] == [sys.executable, "-m", "src.spark.run_pipeline"]
    assert parse_pipeline_args(spark_cmd[3:]).storage_root == str(root)


def test_realtime_refresh_waits_for_live_runner_instead_of_spawning_spark(tmp_path: Path):
    root = tmp_path / "data"
    _write_gold(root, "20261110T080000000000Z")
    heartbeat = root / "lake" / "_runner_heartbeat.json"
    heartbeat.write_text(json.dumps({
        "status": "IDLE",
        "poll_seconds": 15,
        "last_check_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }), encoding="utf-8")
    calls = []

    def fake_run(cmd, *args, **kwargs):
        calls.append(cmd)
        return _ok_proc()

    def fake_sleep(_seconds):
        _write_gold(root, "20261110T081000000000Z", slot="2026-11-10T08:15:00Z")  # runner commits

    with mock.patch("src.dashboard.data_loader.subprocess.run", side_effect=fake_run), \
         mock.patch("src.dashboard.data_loader.time.sleep", side_effect=fake_sleep):
        result = fetch_and_update_realtime(storage_root=str(root))

    assert result["status"] == "SUCCESS"
    assert result["published_by"] == "runner"
    assert result["latest_slot_utc"] == "2026-11-10T08:15:00Z"
    assert [c[2] for c in calls] == ["src.ingestion.run_ingest"]


def test_realtime_refresh_reports_ingestion_error_without_running_spark():
    proc = _ok_proc()
    proc.returncode, proc.stderr = 1, "Network connection timeout to AviationWeather"
    with mock.patch("src.dashboard.data_loader.subprocess.run", return_value=proc) as run:
        result = fetch_and_update_realtime(storage_root="data")
    assert result["status"] == "ERROR"
    assert "Network connection timeout" in result["error"]
    assert run.call_count == 1


def test_realtime_refresh_reports_spark_failure(tmp_path: Path):
    failing = _ok_proc()
    failing.returncode, failing.stderr = 1, "Py4JError: JVM crashed"
    with mock.patch("src.dashboard.data_loader.subprocess.run", side_effect=[_ok_proc(), failing]):
        result = fetch_and_update_realtime(storage_root=str(tmp_path))
    assert result["status"] == "ERROR"
    assert "JVM crashed" in result["error"]


def test_realtime_refresh_handles_ingestion_timeout():
    with mock.patch(
        "src.dashboard.data_loader.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="ingest", timeout=180),
    ) as run:
        result = fetch_and_update_realtime(storage_root="data")
    assert result["status"] == "ERROR"
    assert "timeout" in result["error"].lower()
    assert run.call_count == 1


def test_watcher_flags_only_versions_newer_than_the_loaded_one(tmp_path: Path):
    root = tmp_path / "data"
    _write_gold(root, "20261110T080000000000Z")
    assert has_new_decision_version(str(root)) is False  # nothing loaded yet in this process
    load_decision_slots(str(root))
    assert has_new_decision_version(str(root)) is False
    write_lake_version(root, "decision_table", [], run_id="20261110T081000000000Z", committed=False)
    assert has_new_decision_version(str(root)) is False  # uncommitted write is invisible
    _write_gold(root, "20261110T082000000000Z", slot="2026-11-10T08:25:00Z")
    assert has_new_decision_version(str(root)) is True
    assert max(load_decision_slots(str(root))) == "2026-11-10T08:25:00Z"
    assert has_new_decision_version(str(root)) is False


def test_load_decision_slots_never_writes(tmp_path: Path):
    root = tmp_path / "data"
    _write_gold(root, "20261110T080000000000Z")
    before = sorted((p, p.stat().st_mtime_ns) for p in root.rglob("*"))
    load_decision_slots(str(root))
    assert sorted((p, p.stat().st_mtime_ns) for p in root.rglob("*")) == before


def test_replay_load_is_fast(isolated_data):
    """Loading the real Gold decision table must stay interactive (< 1s)."""
    data_loader._LOADER_CACHE.clear()
    start = time.perf_counter()
    slots = load_decision_slots(str(isolated_data))
    elapsed = time.perf_counter() - start
    assert slots
    assert elapsed < 1.0, f"load_decision_slots took {elapsed:.2f}s"
