"""Replay slots come from config/replay_slots.json, not from hardcoded literals."""

import json
from pathlib import Path

import pytest

from src.spark.replay_slots import DEFAULT_REPLAY_SLOTS, load_replay_slots
from src.spark.run_pipeline import resolve_review_slots


def test_shipped_config_matches_known_replay_slots():
    assert load_replay_slots() == [
        "2026-11-09T07:00:00Z",
        "2026-11-09T08:00:00Z",
        "2026-11-09T11:00:00Z",
    ]


def test_load_replay_slots_sorts_and_dedupes(tmp_path: Path):
    cfg = tmp_path / "slots.json"
    cfg.write_text(json.dumps({"replay_slots_utc": ["2026-11-09T08:00:00Z", "2026-11-09T07:00:00Z", "2026-11-09T08:00:00Z"]}))
    assert load_replay_slots(str(cfg)) == ["2026-11-09T07:00:00Z", "2026-11-09T08:00:00Z"]


def test_load_replay_slots_falls_back_when_missing(tmp_path: Path):
    assert load_replay_slots(str(tmp_path / "nope.json")) == list(DEFAULT_REPLAY_SLOTS)


def test_load_replay_slots_rejects_invalid_timestamp(tmp_path: Path):
    cfg = tmp_path / "slots.json"
    cfg.write_text(json.dumps({"replay_slots_utc": ["not-a-time"]}))
    with pytest.raises(ValueError):
        load_replay_slots(str(cfg))


def test_review_slot_policies_use_live_fetches_rounded_up_to_5_minutes(spark):
    # Timestamps parsed from UTC strings in Spark: createDataFrame would shift naive datetimes by the OS tz.
    df = spark.createDataFrame(
        [("2026-11-10T08:01:30Z", False), ("2026-11-10T08:05:00Z", False),
         ("2026-11-10T08:11:00Z", False), ("2026-11-10T09:59:00Z", True)],
        "f string, is_backfill boolean",
    ).selectExpr("to_timestamp(f, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS fetched_at_utc", "is_backfill")
    replay = ["2026-11-09T07:00:00Z", "2026-11-09T08:00:00Z", "2026-11-09T11:00:00Z"]
    live = ["2026-11-10T08:05:00Z", "2026-11-10T08:15:00Z"]  # back-filled fetches never create slots
    assert resolve_review_slots("all", df) == replay + live
    assert resolve_review_slots("auto", df) == replay + live[-1:]
    assert resolve_review_slots(" 2026-11-09T08:00:00Z , 2026-11-09T07:00:00Z ", df) == [
        "2026-11-09T07:00:00Z",
        "2026-11-09T08:00:00Z",
    ]
