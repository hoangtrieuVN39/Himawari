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


def test_load_decision_slots_and_sort_modes_preserve_co_priority(isolated_data):
    slots = load_decision_slots(str(isolated_data))
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


def test_get_b13_image_sequence_no_future_leakage(isolated_data):
    seq = get_b13_image_sequence(str(isolated_data), "VVDN", "2026-11-09T07:30:00Z", max_frames=6)
    assert 1 <= len(seq) <= 6
    assert seq[-1]["utc_hhmm"] == "07:30Z"
    for item in seq:
        assert item["utc_hhmm"] <= "07:30Z"


def test_get_b13_image_sequence_only_frames_within_60_minutes(tmp_path):
    day_dir = tmp_path / "processed" / "himawari_png" / "airport=VVDN" / "date=2026-11-09"
    day_dir.mkdir(parents=True)
    for name in ("0700", "0750", "1620"):
        (day_dir / f"B13_VVDN_20261109_{name}Z.png").write_bytes(b"")
    next_day = tmp_path / "processed" / "himawari_png" / "airport=VVDN" / "date=2026-11-10"
    next_day.mkdir(parents=True)
    (next_day / "B13_VVDN_20261110_1530Z.png").write_bytes(b"")

    seq = get_b13_image_sequence(str(tmp_path), "VVDN", "2026-11-10T15:45:00Z", max_frames=6)
    assert [f["scan_time_utc"] for f in seq] == ["2026-11-10T15:30:00Z"]  # day-old frames are not "before the event"
    seq_0800 = get_b13_image_sequence(str(tmp_path), "VVDN", "2026-11-09T08:00:00Z", max_frames=6)
    assert [f["utc_hhmm"] for f in seq_0800] == ["07:00Z", "07:50Z"]  # 60-minute window is inclusive


def test_load_audit_and_reconciliation_returns_all_sections(isolated_data):
    audit = load_audit_and_reconciliation(str(isolated_data))
    assert audit["reconciliation"]["all_sources_reconciled"] is True
    assert len(audit["binary_file_errors"]) >= 1
    assert audit["ingestion_summary"]["total_logs"] >= 1
    assert len(audit["synthetic_quarantine_preview"]) >= 1
