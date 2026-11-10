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


def test_resolve_review_slots_auto_adds_latest_live(monkeypatch):
    monkeypatch.setattr("src.spark.run_pipeline.discover_latest_slot_utc", lambda root: "2026-11-10T08:05:00Z")
    slots = resolve_review_slots("auto", "data")
    assert slots == ["2026-11-09T07:00:00Z", "2026-11-09T08:00:00Z", "2026-11-09T11:00:00Z", "2026-11-10T08:05:00Z"]


def test_resolve_review_slots_explicit_list():
    assert resolve_review_slots(" 2026-11-09T08:00:00Z , 2026-11-09T07:00:00Z ", "data") == [
        "2026-11-09T08:00:00Z",
        "2026-11-09T07:00:00Z",
    ]
