"""Signature-based caching of dashboard loaders: no re-read while files are unchanged."""

import json
import os
import time

from src.dashboard import data_loader as dl
from src.dashboard import kpi_evaluator as ke


def _write_decision(root, rows):
    path = root / "processed" / "decision_table" / "decision_table.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return path


def _row(icao, level=4, slot="2026-11-09T07:00:00Z"):
    return {"review_slot_utc": slot, "observed_at_utc": slot, "airport_icao": icao, "priority_level": level}


def _bump(path):
    st = path.stat()
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))


def test_decision_slots_cached_until_file_changes(tmp_path, monkeypatch):
    path = _write_decision(tmp_path, [_row("VVDN", 1), _row("VVNB")])
    calls = []
    real = dl._read_decision_slots
    monkeypatch.setattr(dl, "_read_decision_slots", lambda p: calls.append(p) or real(p))

    first = dl.load_decision_slots(str(tmp_path))
    second = dl.load_decision_slots(str(tmp_path))
    assert len(calls) == 1 and first == second

    _write_decision(tmp_path, [_row("VVDN", 1), _row("VVNB"), _row("VVTS")])
    _bump(path)
    third = dl.load_decision_slots(str(tmp_path))
    assert len(calls) == 2
    assert len(third["2026-11-09T07:00:00Z"]) == 3


def test_decision_slots_callers_cannot_corrupt_the_cache(tmp_path):
    _write_decision(tmp_path, [_row("VVDN", 1)])
    got = dl.load_decision_slots(str(tmp_path))
    got["2026-11-09T07:00:00Z"][0]["priority_level"] = 99
    got["2026-11-09T07:00:00Z"].append({"airport_icao": "XXXX"})
    again = dl.load_decision_slots(str(tmp_path))
    assert again["2026-11-09T07:00:00Z"][0]["priority_level"] == 1
    assert len(again["2026-11-09T07:00:00Z"]) == 1


def test_nested_lists_in_cached_rows_are_protected_too(tmp_path):
    row = {**_row("VVDN", 1), "station_weather_codes": ["RA"], "accepted_first_review_airports": ["VVDN"]}
    _write_decision(tmp_path, [row])
    got = dl.load_decision_slots(str(tmp_path))["2026-11-09T07:00:00Z"][0]
    got["station_weather_codes"].append("TS")
    got["accepted_first_review_airports"].clear()
    again = dl.load_decision_slots(str(tmp_path))["2026-11-09T07:00:00Z"][0]
    assert again["station_weather_codes"] == ["RA"]
    assert again["accepted_first_review_airports"] == ["VVDN"]


def test_kpi_trials_nested_lists_are_protected_too(tmp_path):
    log = tmp_path / "kpi.jsonl"
    ke.append_kpi_trial({"view_mode": "dashboard", "failure_codes": ["WRONG_REASON"]}, log_path=str(log))
    got = ke.load_kpi_trials(str(log))
    got[0]["failure_codes"].append("WRONG_AIRPORT")
    assert ke.load_kpi_trials(str(log))[0]["failure_codes"] == ["WRONG_REASON"]


def test_missing_decision_table_returns_empty_and_recovers(tmp_path):
    assert dl.load_decision_slots(str(tmp_path)) == {}
    _write_decision(tmp_path, [_row("VVDN", 1)])
    assert "2026-11-09T07:00:00Z" in dl.load_decision_slots(str(tmp_path))


def test_audit_cached_until_any_input_changes(tmp_path, monkeypatch):
    log = tmp_path / "logs" / "ingestion_log.jsonl"
    log.parent.mkdir(parents=True)
    log.write_text(json.dumps({"bytes_written": 10, "record_count": 1}) + "\n", encoding="utf-8")
    calls = []
    real = dl._read_audit_and_reconciliation
    monkeypatch.setattr(dl, "_read_audit_and_reconciliation", lambda r: calls.append(r) or real(r))

    a = dl.load_audit_and_reconciliation(str(tmp_path))
    dl.load_audit_and_reconciliation(str(tmp_path))
    assert len(calls) == 1 and a["ingestion_summary"]["total_bytes"] == 10

    with log.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"bytes_written": 5, "record_count": 1}) + "\n")
    b = dl.load_audit_and_reconciliation(str(tmp_path))
    assert len(calls) == 2 and b["ingestion_summary"]["total_bytes"] == 15

    b["ingestion_summary"]["total_bytes"] = -1
    assert dl.load_audit_and_reconciliation(str(tmp_path))["ingestion_summary"]["total_bytes"] == 15


def test_has_unsynced_raw_data_compares_signature_without_writing(tmp_path, monkeypatch):
    key = str(tmp_path.resolve())
    sig = {"v": (("a.json", 1, 10),)}
    monkeypatch.setattr(dl, "_raw_input_signature", lambda root: sig["v"])

    assert dl.has_unsynced_raw_data(str(tmp_path)) is False  # nothing synced yet: next full run syncs
    monkeypatch.setitem(dl._SYNC_CACHE, key, {"input_sig": sig["v"], "dec_sig": (0, 0)})
    assert dl.has_unsynced_raw_data(str(tmp_path)) is False
    sig["v"] = (("a.json", 1, 10), ("b.json", 2, 20))  # ingestion wrote a new raw batch
    assert dl.has_unsynced_raw_data(str(tmp_path)) is True
    monkeypatch.setitem(dl._SYNC_CACHE, key, {"input_sig": sig["v"], "dec_sig": (0, 0)})  # a sync caught up
    assert dl.has_unsynced_raw_data(str(tmp_path)) is False


def test_kpi_trials_cached_and_refreshed_after_append(tmp_path, monkeypatch):
    log = tmp_path / "kpi.jsonl"
    calls = []
    real = ke._read_kpi_trials
    monkeypatch.setattr(ke, "_read_kpi_trials", lambda p, s: calls.append(p) or real(p, s))

    assert ke.load_kpi_trials(str(log)) == []
    ke.append_kpi_trial({"view_mode": "dashboard", "is_passed": True}, log_path=str(log))
    first = ke.load_kpi_trials(str(log))
    ke.load_kpi_trials(str(log))
    assert len(first) == 1 and len(calls) == 1  # missing file is not read; second call hits the cache

    ke.append_kpi_trial({"view_mode": "dashboard", "is_passed": False}, log_path=str(log))
    assert len(ke.load_kpi_trials(str(log))) == 2
    seeds = ke.load_kpi_trials(str(log), include_seeds=False)
    assert len(seeds) == 2
