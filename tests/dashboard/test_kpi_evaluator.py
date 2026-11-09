"""Unit tests for KPI trial evaluator, reason matcher, and median statistics."""

from src.dashboard.data_loader import load_decision_slots, sort_slot_rows
from src.dashboard.kpi_evaluator import (
    REASON_OPTIONS,
    derive_expected_reason_code,
    evaluate_kpi_submission,
    matches_observation_time,
    compute_kpi_summary,
    append_kpi_trial,
    load_kpi_trials,
)


def test_evaluate_single_answer_slot_0700():
    slots = load_decision_slots("data")
    row_07 = slots["2026-11-09T07:00:00Z"]

    # Correct airport (VVDN), correct reason, UTC time
    res_utc = evaluate_kpi_submission(
        row_07,
        selected_airport="VVDN",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="07:00Z",
        elapsed_seconds=38.5,
    )
    assert res_utc["is_passed"] is True

    # Correct airport (VVDN), correct reason, Vietnam time (14:00)
    res_vn = evaluate_kpi_submission(
        row_07,
        selected_airport="VVDN",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="14:00",
        elapsed_seconds=35.0,
    )
    assert res_vn["is_passed"] is True

    # Wrong airport (VVTS)
    res_wrong = evaluate_kpi_submission(
        row_07,
        selected_airport="VVTS",
        selected_reason_code="ROUTINE",
        entered_obs_time="07:00Z",
        elapsed_seconds=40.0,
    )
    assert res_wrong["is_passed"] is False
    assert "WRONG_AIRPORT" in res_wrong["failure_codes"]


def test_evaluate_co_priority_slot_0800_order_independent():
    slots = load_decision_slots("data")
    row_08_reversed = sort_slot_rows(slots["2026-11-09T08:00:00Z"], "icao_desc")

    # Both VVDN and VVNB must be accepted even when table order is reversed
    res_vvdn = evaluate_kpi_submission(
        row_08_reversed,
        selected_airport="VVDN",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="090800Z",
        elapsed_seconds=42.0,
    )
    res_vvnb = evaluate_kpi_submission(
        row_08_reversed,
        selected_airport="VVNB",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="15:00",
        elapsed_seconds=39.0,
    )
    assert res_vvdn["is_passed"] is True
    assert res_vvnb["is_passed"] is True


def test_compute_kpi_summary_calculates_median_and_reduction(tmp_path):
    log_file = tmp_path / "kpi_trials.jsonl"
    sample_trials = [
        {"view_mode": "baseline_3_sources", "is_passed": True, "elapsed_seconds": 95.0, "status": "COMPLETED"},
        {"view_mode": "baseline_3_sources", "is_passed": True, "elapsed_seconds": 105.0, "status": "COMPLETED"},
        {"view_mode": "baseline_3_sources", "is_passed": False, "elapsed_seconds": 120.0, "status": "COMPLETED"},
        {"view_mode": "dashboard", "is_passed": True, "elapsed_seconds": 40.0, "status": "COMPLETED"},
        {"view_mode": "dashboard", "is_passed": True, "elapsed_seconds": 50.0, "status": "COMPLETED"},
    ]
    for t in sample_trials:
        append_kpi_trial(t, str(log_file))

    loaded = load_kpi_trials(str(log_file))
    summary = compute_kpi_summary(loaded)
    assert summary["baseline_3_sources"]["median_passed_seconds"] == 100.0
    assert summary["baseline_3_sources"]["passed_trials"] == 2
    assert summary["baseline_3_sources"]["total_trials"] == 3
    assert summary["dashboard"]["median_passed_seconds"] == 45.0
    assert summary["time_reduction_pct"] == 55.0
    assert summary["meets_30_pct_target"] is True


def test_derive_expected_reason_code_all_levels():
    assert derive_expected_reason_code({"has_precip_or_thunder_at_station": True}) == "PRECIP_OR_THUNDER_AT_STATION"
    assert derive_expected_reason_code({"is_low_visibility": True}) == "LOW_VISIBILITY"
    assert derive_expected_reason_code({"visibility_m": 3500}) == "LOW_VISIBILITY"
    assert derive_expected_reason_code({"metar_is_present": False}) == "STALE_OR_MISSING_METAR"
    assert derive_expected_reason_code({"metar_age_minutes": 75.0}) == "STALE_OR_MISSING_METAR"
    assert derive_expected_reason_code({"forecast_rain_next_hour_mm": 0.5}) == "FORECAST_RAIN_NEXT_HOUR"
    assert derive_expected_reason_code({}) == "ROUTINE"


def test_matches_observation_time_various_formats():
    row = {
        "obs_utc_hhmm": "07:00Z",
        "obs_vn_hhmm": "14:00",
        "obs_ddhhmmz": "090700Z",
    }
    # UTC formats
    assert matches_observation_time("07:00Z", row) is True
    assert matches_observation_time("0700Z", row) is True
    assert matches_observation_time("090700Z", row) is True
    assert matches_observation_time("07:00", row) is True
    assert matches_observation_time("7:00", row) is True

    # VN formats
    assert matches_observation_time("14:00", row) is True
    assert matches_observation_time("14h00", row) is True
    assert matches_observation_time("14h", row) is True
    assert matches_observation_time("14", row) is True

    # Mismatch formats
    assert matches_observation_time("14:30", row) is False
    assert matches_observation_time("08:00Z", row) is False
    assert matches_observation_time("", row) is False
    assert matches_observation_time("abc", row) is False


def test_evaluate_kpi_submission_abandoned_and_failures():
    slots = load_decision_slots("data")
    row_07 = slots["2026-11-09T07:00:00Z"]

    # Abandoned trial
    res_abandoned = evaluate_kpi_submission(
        row_07,
        selected_airport="VVDN",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="07:00Z",
        elapsed_seconds=15.0,
        status="ABANDONED",
    )
    assert res_abandoned["is_passed"] is False
    assert "ABANDONED" in res_abandoned["failure_codes"]

    # Wrong reason
    res_wrong_reason = evaluate_kpi_submission(
        row_07,
        selected_airport="VVDN",
        selected_reason_code="ROUTINE",
        entered_obs_time="07:00Z",
        elapsed_seconds=30.0,
    )
    assert res_wrong_reason["is_passed"] is False
    assert "WRONG_REASON" in res_wrong_reason["failure_codes"]

    # Wrong observation time
    res_wrong_time = evaluate_kpi_submission(
        row_07,
        selected_airport="VVDN",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="12:00",
        elapsed_seconds=30.0,
    )
    assert res_wrong_time["is_passed"] is False
    assert "WRONG_OBS_TIME" in res_wrong_time["failure_codes"]


def test_seeded_kpi_trials_log_loads_and_meets_target():
    trials = load_kpi_trials("data/logs/kpi_trials_log.jsonl")
    assert len(trials) == 8

    # Check non-seed filter
    assert len(load_kpi_trials("data/logs/kpi_trials_log.jsonl", include_seeds=False)) == 0

    summary = compute_kpi_summary(trials)
    assert summary["baseline_3_sources"]["total_trials"] == 4
    assert summary["baseline_3_sources"]["passed_trials"] == 4
    assert summary["baseline_3_sources"]["median_passed_seconds"] == 102.0

    assert summary["dashboard"]["total_trials"] == 4
    assert summary["dashboard"]["passed_trials"] == 4
    assert summary["dashboard"]["median_passed_seconds"] == 44.0

    # Reduction: (102.0 - 44.0) / 102.0 = 56.86% -> 56.9%
    assert summary["time_reduction_pct"] == 56.9
    assert summary["meets_30_pct_target"] is True
