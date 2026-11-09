"""Unit tests for dashboard data loader, time normalization, and audit loader."""

from src.dashboard.data_loader import (
    normalize_timestamp_pair,
    load_decision_slots,
    sort_slot_rows,
    get_b13_image_sequence,
    load_audit_and_reconciliation,
)


def test_normalize_timestamp_pair_recovers_utc_from_raw_ob():
    info = normalize_timestamp_pair(
        "2026-11-09T14:00:00",
        raw_ob="METAR VVDN 090700Z 09008KT 8000 -RA",
    )
    assert info["utc_iso"] == "2026-11-09T07:00:00Z"
    assert info["utc_hhmm"] == "07:00Z"
    assert info["vn_hhmm"] == "14:00"
    assert info["metar_ddhhmmz"] == "090700Z"


def test_load_decision_slots_and_sort_modes_preserve_co_priority():
    slots = load_decision_slots("data")
    assert "2026-11-09T07:00:00Z" in slots
    assert "2026-11-09T08:00:00Z" in slots
    assert "2026-11-09T16:45:00Z" in slots

    slot_08 = slots["2026-11-09T08:00:00Z"]
    assert len(slot_08) == 3

    slot_live = slots["2026-11-09T16:45:00Z"]
    assert len(slot_live) == 3
    for r in slot_live:
        assert r["slot_utc_hhmm"] == "16:45Z"
        assert r["slot_vn_hhmm"] == "23:45"
        assert r["obs_utc_hhmm"] == "16:30Z"
        assert r["obs_vn_hhmm"] == "23:30"
        assert r["forecast_is_present"] is True

    asc = sort_slot_rows(slot_08, "icao_asc")
    desc = sort_slot_rows(slot_08, "icao_desc")
    assert [r["airport_icao"] for r in asc] == ["VVDN", "VVNB", "VVTS"]
    assert [r["airport_icao"] for r in desc] == ["VVTS", "VVNB", "VVDN"]
    for r in desc:
        assert r["accepted_first_review_airports"] == ["VVDN", "VVNB"]


def test_get_b13_image_sequence_no_future_leakage():
    seq = get_b13_image_sequence("data", "VVDN", "2026-11-09T07:30:00Z", max_frames=6)
    assert 1 <= len(seq) <= 6
    assert seq[-1]["utc_hhmm"] == "07:30Z"
    for item in seq:
        assert item["utc_hhmm"] <= "07:30Z"


def test_load_audit_and_reconciliation_returns_all_sections():
    audit = load_audit_and_reconciliation("data")
    assert audit["reconciliation"]["all_sources_reconciled"] is True
    assert len(audit["binary_file_errors"]) >= 1
    assert audit["ingestion_summary"]["total_logs"] >= 1
    assert len(audit["synthetic_quarantine_preview"]) >= 1
