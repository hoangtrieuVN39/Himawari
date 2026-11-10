"""KPI trial state machine: what each timer/submit action does to the session (no Streamlit)."""

from src.dashboard.views.kpi_state import NOT_STARTED_MSG, apply_trial_action, validate_client_elapsed

ANSWERS = {"kpi_airport": "VVDN", "kpi_reason": "ROUTINE", "kpi_obs_time": "07:00Z"}


def _state(**kw):
    s = {"timer_state": "idle", "timer_start": 0.0, "timer_elapsed": 0.0, "timer_has_started": False, "last_kpi_result": None}
    s.update(kw)
    return s


def _act(state, type_, now, client_ts=None, nonce=None, view_mode="baseline_3_sources", answers=None):
    action = {"type": type_, "nonce": nonce or f"{type_}-{now}", "client_ts": client_ts}
    return apply_trial_action(state, action, now=now, view_mode=view_mode, answers=answers if answers is not None else ANSWERS)


def test_submit_without_start_records_nothing():
    s = _state()
    out = _act(s, "submit", 10.0)
    assert out.submit is None and out.warning == NOT_STARTED_MSG


def test_start_then_submit_records_server_and_client_elapsed():
    s = _state(kpi_airport="VVNB")
    start = _act(s, "start", 100.0, client_ts=1_000_000)
    assert start.clear_answers and start.rerun_app
    assert s["timer_state"] == "running" and s["trial_view_mode"] == "baseline_3_sources"
    out = _act(s, "submit", 112.5, client_ts=1_012_300)
    assert out.submit.server_elapsed == 12.5
    assert out.submit.client_elapsed == 12.3
    assert out.submit.view_mode == "baseline_3_sources"
    assert s["timer_has_started"] is False and "trial_view_mode" not in s and out.rerun_app


def test_stopped_timer_uses_stopped_durations():
    s = _state()
    _act(s, "start", 100.0, client_ts=0)
    _act(s, "stop", 112.5, client_ts=12_400)
    out = _act(s, "submit", 150.0, client_ts=50_000)
    assert out.submit.server_elapsed == 12.5 and out.submit.client_elapsed == 12.4


def test_reset_then_submit_records_nothing_and_unlocks_mode():
    s = _state()
    _act(s, "start", 100.0, client_ts=0)
    reset = _act(s, "reset", 105.0, client_ts=5_000)
    assert reset.clear_answers and "trial_view_mode" not in s
    assert _act(s, "submit", 106.0, client_ts=6_000).submit is None


def test_second_submit_needs_a_new_start():
    s = _state()
    _act(s, "start", 100.0, client_ts=0)
    assert _act(s, "submit", 110.0, client_ts=10_000).submit is not None
    again = _act(s, "submit", 111.0, client_ts=11_000)
    assert again.submit is None and again.warning == NOT_STARTED_MSG


def test_duplicate_nonce_is_ignored():
    s = _state()
    _act(s, "start", 100.0, client_ts=0, nonce="n1")
    first = _act(s, "submit", 110.0, client_ts=10_000, nonce="n2")
    _act(s, "start", 120.0, client_ts=20_000, nonce="n3")
    replay = _act(s, "submit", 130.0, client_ts=30_000, nonce="n2")
    assert first.submit is not None and replay.submit is None and not replay.handled


def test_action_without_a_nonce_is_rejected():
    s = _state()
    _act(s, "start", 100.0, client_ts=0)
    for bad in (None, "", 0, 123):
        out = apply_trial_action(s, {"type": "submit", "nonce": bad, "client_ts": 10_000}, now=110.0,
                                 view_mode="baseline_3_sources", answers=ANSWERS)
        assert out.handled is False and out.submit is None
    out = apply_trial_action(s, {"type": "submit", "client_ts": 10_000}, now=110.0,
                             view_mode="baseline_3_sources", answers=ANSWERS)
    assert out.handled is False and s["timer_has_started"] is True  # trial still open, nothing recorded


def test_answers_required_and_trial_stays_open():
    s = _state()
    _act(s, "start", 100.0, client_ts=0)
    no_airport = _act(s, "submit", 105.0, client_ts=5_000, answers={"kpi_reason": "ROUTINE"})
    assert no_airport.submit is None and "sân bay" in no_airport.warning
    no_reason = _act(s, "submit", 106.0, client_ts=6_000, answers={"kpi_airport": "VVDN"})
    assert no_reason.submit is None and "lý do" in no_reason.warning
    assert s["timer_has_started"] is True and s["timer_state"] == "running"


def test_trial_mode_frozen_at_start():
    s = _state()
    _act(s, "start", 100.0, client_ts=0, view_mode="dashboard")
    out = _act(s, "submit", 110.0, client_ts=10_000, view_mode="baseline_3_sources")
    assert out.submit.view_mode == "dashboard"


def test_client_elapsed_validation():
    assert validate_client_elapsed(12.3, 12.5) == 12.3
    assert validate_client_elapsed(14.4, 12.5) == 14.4  # within +2 s of the server measure
    assert validate_client_elapsed(15.0, 12.5) is None
    assert validate_client_elapsed(-0.1, 12.5) is None
    assert validate_client_elapsed(float("nan"), 12.5) is None
    assert validate_client_elapsed(None, 12.5) is None
    s = _state()
    _act(s, "start", 100.0, client_ts=None)
    assert _act(s, "submit", 110.0, client_ts=10_000).submit.client_elapsed is None
