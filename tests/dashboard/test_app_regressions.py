"""Regression tests for dashboard UI rendering, missing data handling, and timer lifecycle."""

from unittest import mock
import pytest
from src.dashboard import app
from src.dashboard.views.kpi import NOT_STARTED_MSG as NOT_STARTED


def _create_mock_slot_rows(missing_indices: list[int] = [0]):
    """Returns 3 airport rows (VVDN, VVNB, VVTS) where airports at missing_indices have missing METAR."""
    rows = []
    airports = [("VVDN", "Quốc tế Đà Nẵng"), ("VVNB", "Quốc tế Nội Bài"), ("VVTS", "Quốc tế Tân Sơn Nhất")]
    for idx, (icao, name) in enumerate(airports):
        if idx in missing_indices:
            rows.append({
                "airport_icao": icao,
                "airport_name": name,
                "priority_level": 2,
                "reason_vi": "Khuyết bản tin METAR",
                "obs_utc_hhmm": "N/A",
                "obs_vn_hhmm": "N/A",
                "metar_is_present": False,
                "metar_age_minutes": None,
                "visibility_m": None,
                "forecast_rain_next_hour_mm": 0.0,
                "b13_is_present": True,
                "b13_summary_vi": "Bình thường",
                "inner_bt_min_c": 25.0,
                "raw_ob": "",
                "station_weather_codes": None,
                "vicinity_weather_codes": None,
                "trend_weather_codes": None,
                "forecast_fetched_at_utc": "2026-11-10T08:00:00Z",
            })
        else:
            rows.append({
                "airport_icao": icao,
                "airport_name": name,
                "priority_level": 4,
                "reason_vi": "Thời tiết bình thường",
                "obs_utc_hhmm": "08:00Z",
                "obs_vn_hhmm": "15:00",
                "metar_is_present": True,
                "metar_age_minutes": 15.0,
                "visibility_m": 10000,
                "forecast_rain_next_hour_mm": 0.0,
                "b13_is_present": True,
                "b13_summary_vi": "Bình thường",
                "inner_bt_min_c": 25.0,
                "raw_ob": f"METAR {icao} 100800Z 09010KT 9999 FEW020 28/24 Q1012",
                "station_weather_codes": None,
                "vicinity_weather_codes": None,
                "trend_weather_codes": None,
                "forecast_fetched_at_utc": "2026-11-10T08:00:00Z",
            })
    return rows


@pytest.mark.parametrize("missing_indices", [[0], [0, 1, 2]])
def test_missing_metar_renders_without_exception(missing_indices):
    mock_rows = _create_mock_slot_rows(missing_indices)
    mock_slots = {"2026-11-10T08:00:00Z": mock_rows}

    with mock.patch("streamlit.set_page_config"), \
         mock.patch("streamlit.rerun"), \
         mock.patch("src.dashboard.app.load_decision_slots", return_value=mock_slots), \
         mock.patch("src.dashboard.app.load_audit_and_reconciliation", return_value={
             "reconciliation": {"all_sources_reconciled": True},
             "binary_file_errors": [],
             "ingestion_summary": {},
             "synthetic_quarantine_preview": [],
         }), \
         mock.patch("src.dashboard.app.load_kpi_trials", return_value=[]):

        # Calling app.main() directly must not raise TypeError when formatting metar_age_minutes=None
        app.main()


def test_format_metar_age_helper():
    assert hasattr(app, "_format_metar_age"), "app must provide _format_metar_age helper"
    assert app._format_metar_age(None) == "N/A"
    assert app._format_metar_age(0.0) == "0 phút"
    assert app._format_metar_age(15.4) == "15 phút"
    assert app._format_metar_age(60.0) == "60 phút"


def _run_app_with_session(session_state_dict: dict, submit_clicked: bool = False, clicked_labels: tuple = ()):
    import streamlit as st
    mock_append = mock.MagicMock()
    mock_warning = mock.MagicMock()

    mock_rows = _create_mock_slot_rows([])
    mock_slots = {"2026-11-09T07:00:00Z": mock_rows}

    import uuid
    from src.dashboard.views import kpi as kpi_view

    clicks = {"Bắt đầu": "start", "Dừng": "stop", "Đặt lại": "reset"}

    def fake_trial_controls(mode, key, **data):
        """Stands in for the browser component: returns the action a click would have triggered."""
        if mode == "timer":
            for token, kind in clicks.items():
                if any(token in label for label in clicked_labels):
                    return {"type": kind, "nonce": str(uuid.uuid4()), "client_ts": 1_000_000}
            return None
        return {"type": "submit", "nonce": str(uuid.uuid4()), "client_ts": 1_012_500} if submit_clicked else None

    # A complete answer is filled in (bare-mode widgets would otherwise return their empty default);
    # these tests are about the timer lifecycle, not grading.
    answers = {"kpi_airport": "VVDN", "kpi_reason": "ROUTINE"}
    real_segmented, real_selectbox = st.segmented_control, st.selectbox

    def _answer(real):
        def widget(label, *args, key=None, **kwargs):
            if key in answers:
                return answers[key]
            return real(label, *args, key=key, **kwargs)
        return widget

    # The KPI form lives in a lazily-rendered tab; these tests exercise it as if that tab were selected.
    with mock.patch("streamlit.set_page_config"), \
         mock.patch("streamlit.segmented_control", side_effect=_answer(real_segmented)), \
         mock.patch("streamlit.selectbox", side_effect=_answer(real_selectbox)), \
         mock.patch("streamlit.rerun"), \
         mock.patch("src.dashboard.app._tab_active", return_value=True), \
         mock.patch("src.dashboard.views.kpi.trial_controls", side_effect=fake_trial_controls), \
         mock.patch("src.dashboard.views.kpi._submission_form", kpi_view._submission_form_impl), \
         mock.patch("streamlit.warning", mock_warning), \
         mock.patch("src.dashboard.app.load_decision_slots", return_value=mock_slots), \
         mock.patch("src.dashboard.app.load_audit_and_reconciliation", return_value={
             "reconciliation": {"all_sources_reconciled": True},
             "binary_file_errors": [],
             "ingestion_summary": {},
             "synthetic_quarantine_preview": [],
         }), \
         mock.patch("src.dashboard.app.load_kpi_trials", return_value=[]), \
         mock.patch("src.dashboard.app.append_kpi_trial", mock_append):

        for k, v in {**answers, **session_state_dict}.items():
            st.session_state[k] = v

        app.main()

    return mock_append, mock_warning


def test_submit_without_start_does_not_record_trial():
    # Idle state: user clicks submit without clicking Start
    session = {
        "timer_state": "idle",
        "timer_has_started": False,
        "timer_elapsed": 0.0,
        "timer_start": 0.0,
        "last_kpi_result": None,
    }
    mock_append, mock_warning = _run_app_with_session(session, submit_clicked=True)
    assert not mock_append.called, "Must not append trial when timer was never started"
    assert any(NOT_STARTED in str(c) for c in mock_warning.call_args_list)


def test_submit_after_reset_does_not_record_trial():
    # User had started (timer running), then clicks Reset and Submit in the same run
    session = {
        "timer_state": "running",
        "timer_has_started": True,
        "timer_elapsed": 0.0,
        "timer_start": 100.0,
        "last_kpi_result": None,
    }
    mock_append, mock_warning = _run_app_with_session(
        session, submit_clicked=True, clicked_labels=("Đặt lại",)
    )
    assert not mock_append.called, "Must not append trial after timer was reset"
    assert any(NOT_STARTED in str(c) for c in mock_warning.call_args_list)


def test_submit_with_started_timer_records_monotonic_duration():
    import time
    # Timer was started at 100.0, Submit clicked at 112.5 -> duration = 12.5s
    session = {
        "timer_state": "running",
        "timer_has_started": True,
        "timer_start": 100.0,
        "timer_elapsed": 0.0,
        "last_kpi_result": None,
        "participant_id": "TEST_DISPATCHER",
    }
    with mock.patch("time.perf_counter", return_value=112.5):
        mock_append, mock_warning = _run_app_with_session(session, submit_clicked=True)

    assert mock_append.called, "Must record trial when timer was started"
    record = mock_append.call_args[0][0]
    assert record["elapsed_seconds"] == 12.5
    assert record["is_sample_seed"] is False
    assert not any(NOT_STARTED in str(c) for c in mock_warning.call_args_list)


def test_submit_after_stopped_timer_records_stopped_duration():
    # Timer was started at 100.0, stopped at 112.5 (elapsed = 12.5), submit at 150.0
    session = {
        "timer_state": "stopped",
        "timer_has_started": True,
        "timer_start": 100.0,
        "timer_elapsed": 12.5,
        "last_kpi_result": None,
        "participant_id": "TEST_DISPATCHER",
    }
    with mock.patch("time.perf_counter", return_value=150.0):
        mock_append, mock_warning = _run_app_with_session(session, submit_clicked=True)

    assert mock_append.called
    record = mock_append.call_args[0][0]
    assert record["elapsed_seconds"] == 12.5




def test_second_submit_without_new_start_does_not_record_duplicate():
    """After a trial is recorded the timer must be re-armed with 'Bắt đầu nhận ca' before another submit."""
    session = {
        "timer_state": "stopped",
        "timer_has_started": True,
        "timer_start": 100.0,
        "timer_elapsed": 12.5,
        "last_kpi_result": None,
        "participant_id": "TEST_DISPATCHER",
    }
    mock_append, _ = _run_app_with_session(session, submit_clicked=True)
    assert mock_append.call_count == 1

    # Same session (state persists), second click on Submit without pressing Start again
    mock_append2, mock_warning2 = _run_app_with_session({}, submit_clicked=True)
    assert not mock_append2.called
    assert any(NOT_STARTED in str(c) for c in mock_warning2.call_args_list)

def test_slot_hhmm_derives_labels_without_hardcoded_defaults():
    assert app._slot_hhmm("2026-11-10T08:05:00Z", None) == ("08:05Z", "15:05")
    row = {"slot_utc_hhmm": "07:00Z", "slot_vn_hhmm": "14:00"}
    assert app._slot_hhmm("2026-11-09T07:00:00Z", row) == ("07:00Z", "14:00")
