"""Signature-based caching of dashboard loaders: no re-read while inputs are unchanged."""

import json

from src.dashboard import data_loader as dl
from src.dashboard import kpi_evaluator as ke
from tests.dashboard.lake_fixtures import write_lake_version

_RUN_IDS = iter(f"20261109T07{m:02d}00000000Z" for m in range(60))


def _write_decision(root, rows):
    """Commits `rows` as a new Gold decision_table version (newer than any previous call)."""
    return write_lake_version(root, "decision_table", rows, run_id=next(_RUN_IDS))


def _row(icao, level=4, slot="2026-11-09T07:00:00Z"):
    return {"review_slot_utc": slot, "observed_at_utc": slot, "airport_icao": icao, "priority_level": level}


def test_decision_slots_cached_until_a_new_version_is_committed(tmp_path, monkeypatch):
    _write_decision(tmp_path, [_row("VVDN", 1), _row("VVNB")])
    calls = []
    real = dl._read_decision_slots
    monkeypatch.setattr(dl, "_read_decision_slots", lambda p: calls.append(p) or real(p))

    first = dl.load_decision_slots(str(tmp_path))
    second = dl.load_decision_slots(str(tmp_path))
    assert len(calls) == 1 and first == second

    _write_decision(tmp_path, [_row("VVDN", 1), _row("VVNB"), _row("VVTS")])
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


def test_audit_reads_reconciliation_and_quarantine_from_lake(tmp_path):
    write_lake_version(tmp_path, "reconciliation", [
        {"run_id": "r1", "generated_at_utc": "2026-11-09T08:00:00Z", "all_sources_reconciled": True,
         "source": "metar", "rows_read": 5, "rows_quarantined": 1, "rows_valid_pre_dedup": 4,
         "rows_retained_valid": 3, "rows_duplicate_dropped": 1, "is_reconciled": True,
         "binary_errors_total": 2, "binary_errors_synthetic": 1, "binary_errors_log_path": "x.jsonl"},
    ])
    write_lake_version(tmp_path, "quarantine", [{"source": "metar", "error_codes": ["UNKNOWN_AIRPORT"]}])
    audit = dl.load_audit_and_reconciliation(str(tmp_path))
    rec = audit["reconciliation"]
    assert rec["all_sources_reconciled"] is True
    assert rec["sources"][0]["rows_read"] == rec["sources"][0]["rows_valid_pre_dedup"] + 1
    assert rec["binary_file_errors"] == {"total_count": 2, "synthetic_count": 1, "log_path": "x.jsonl"}
    assert audit["quarantine_records"] == [{"source": "metar", "error_codes": ["UNKNOWN_AIRPORT"]}]


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
