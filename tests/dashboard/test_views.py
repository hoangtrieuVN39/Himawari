"""Rendered-UI checks for the redesigned sidebar, header and the three tabs (streamlit AppTest)."""

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.dashboard.views import baseline_trial_in_progress
from src.dashboard.views.kpi import NOT_STARTED_MSG, interpret_obs_time


def _entrypoint(storage_root: str):
    from src.dashboard import app as dashboard_app

    dashboard_app.STORAGE_ROOT = storage_root
    dashboard_app.main()


def _html(at) -> str:
    return "\n".join(el.proto.body for el in at.get("html") if not el.proto.body.startswith("<style>"))


def _app(root: Path) -> AppTest:
    return AppTest.from_function(_entrypoint, args=(str(root),), default_timeout=120)


def _replay(at: AppTest, slot: str) -> AppTest:
    at.session_state["op_mode"] = "Phát lại"
    at.session_state["slot_replay"] = slot
    return at.run()


def _select_tab(at: AppTest, index: int) -> AppTest:
    at.session_state["main_tabs"] = list(at.tabs)[index].label
    return at.run()


@pytest.fixture
def at(isolated_data):
    return _app(isolated_data)


def test_live_mode_sidebar_header_and_admin_area(at):
    at.run()
    assert not at.exception
    html = _html(at)
    assert "Điều phối Thời tiết" in html and "ĐIỀU PHỐI" in html
    assert "mode-live" in html  # header pill
    assert [t.label for t in at.tabs] == ["Quyết định", "Đo KPI", "Đối soát dữ liệu"]
    labels = [b.label for b in at.button]
    assert "Cập nhật dữ liệu realtime" in labels and "Chạy lại pipeline PySpark" in labels
    admin = next(e for e in at.sidebar.expander if e.label == "Quản trị kỹ thuật")
    assert [b.label for b in admin.button] == ["Chạy lại pipeline PySpark"]
    assert [cb.label for cb in admin.checkbox] == ["Bơm dữ liệu lỗi kiểm thử"]


def test_replay_0700_single_first_review_airport(at):
    at.run()
    _replay(at, "2026-11-09T07:00:00Z")
    assert not at.exception
    html = _html(at)
    assert "PHÁT LẠI" in html
    assert "SÂN BAY CẦN RÀ SOÁT ĐẦU TIÊN" in html and "ĐỒNG MỨC" not in html
    assert 'ban-code big">VVDN<' in html
    assert "VÌ SAO XẾP MỨC NÀY" in html and "JMA/NOAA Himawari-9 B13" in html


def test_replay_0800_co_priority_banner_survives_reordering(at):
    at.run()
    _replay(at, "2026-11-09T08:00:00Z")
    html = _html(at)
    assert "ĐỒNG MỨC ƯU TIÊN ĐẦU" in html and "VVDN" in html and "VVNB" in html
    at.session_state["sort_mode"] = "Mã sân bay Z→A"
    at.run()
    assert not at.exception
    assert "ĐỒNG MỨC ƯU TIÊN ĐẦU" in _html(at)


def test_decision_tab_hidden_during_baseline_trial(at):
    at.run()
    at.session_state["timer_has_started"] = True
    at.session_state["timer_state"] = "running"
    at.session_state["kpi_view_mode"] = "baseline_3_sources"
    at.run()
    html = _html(at)
    assert "Tạm ẩn trong lượt thử Baseline" in html
    assert "SÂN BAY CẦN RÀ SOÁT ĐẦU TIÊN" not in html and "airports" not in html
    # Switching the selector mid-trial must not unlock answers: the trial's mode is frozen at Start.
    at.session_state["trial_view_mode"] = "baseline_3_sources"
    at.session_state["kpi_view_mode"] = "dashboard"
    at.run()
    assert "Tạm ẩn trong lượt thử Baseline" in _html(at)
    # Submitting/resetting closes the trial and unlocks the tab.
    at.session_state["timer_has_started"] = False
    at.run()
    assert "Tạm ẩn trong lượt thử Baseline" not in _html(at)


def test_drill_down_switches_airport_and_uses_client_side_viewer(at):
    # AppTest always reruns the whole script, so fragment isolation is verified in a real browser
    # (.harness/2026-11-11-perf/logs/measure_after.json); here we check the behaviour.
    at.run()
    _replay(at, "2026-11-09T08:00:00Z")
    picker = next(g for g in at.get("button_group") if "VVNB" in str(g.proto))
    picker.set_value("VVNB").run()
    assert not at.exception
    assert "METAR gốc · VVNB" in _html(at)
    iframes = [el for el in at.get("iframe")] if at.get("iframe") else []
    assert iframes, "B13 frames must be rendered by the client-side viewer iframe"
    assert not [s for s in at.get("slider")] and not [s for s in at.get("select_slider")]


def test_satellite_fallback_only_inside_60_minute_window():
    from src.dashboard.views.decision import _scan_within_window

    assert _scan_within_window("2026-11-10T15:30:00Z", "2026-11-10T15:45:00Z") is True
    assert _scan_within_window("2026-11-09T16:20:00Z", "2026-11-10T15:45:00Z") is False
    assert _scan_within_window("2026-11-10T16:00:00Z", "2026-11-10T15:45:00Z") is False  # future frame
    assert _scan_within_window(None, "2026-11-10T15:45:00Z") is False


def test_baseline_trial_flag():
    assert baseline_trial_in_progress({"timer_has_started": True}) is True  # default view is Baseline
    assert baseline_trial_in_progress({"timer_has_started": True, "kpi_view_mode": "dashboard"}) is False
    assert baseline_trial_in_progress({"timer_has_started": False}) is False
    frozen = {"timer_has_started": True, "trial_view_mode": "baseline_3_sources", "kpi_view_mode": "dashboard"}
    assert baseline_trial_in_progress(frozen) is True


def test_open_trial_locks_the_mode_switch_in_the_ui(at):
    # Start/Reset are browser-component clicks (AppTest cannot click them); the state transitions are
    # covered by test_kpi_state.py and test_app_regressions.py. Here: what an open trial looks like.
    at.run()
    _replay(at, "2026-11-09T07:00:00Z")
    _select_tab(at, 1)
    assert not any("chế độ xem bị khóa" in cap.value for cap in at.caption)
    at.session_state["timer_has_started"] = True
    at.session_state["timer_state"] = "running"
    at.session_state["trial_view_mode"] = "baseline_3_sources"
    at.run()
    assert not at.exception
    assert any("chế độ xem bị khóa" in cap.value for cap in at.caption)
    mode_switch = next(g for g in at.get("button_group") if "Baseline" in str(g.proto))
    assert mode_switch.proto.disabled is True
    # Closing the trial (submit/reset) unlocks it again.
    at.session_state["timer_has_started"] = False
    at.session_state["timer_state"] = "idle"
    del at.session_state["trial_view_mode"]
    at.run()
    assert not any("chế độ xem bị khóa" in cap.value for cap in at.caption)


def test_kpi_tab_renders_form_without_recording_anything(at, isolated_data, monkeypatch):
    log = isolated_data / "logs" / "kpi_trials_test.jsonl"
    from src.dashboard import app, kpi_evaluator

    real_append = kpi_evaluator.append_kpi_trial
    monkeypatch.setattr(app, "append_kpi_trial", lambda rec: real_append(rec, log_path=str(log)))
    at.run()
    _replay(at, "2026-11-09T07:00:00Z")
    _select_tab(at, 1)
    assert not at.exception
    html = _html(at)
    assert "Bài thử quyết định có bấm giờ" in html and "Phiếu nộp kết quả" in html
    # No answer is pre-selected and the reason dropdown starts on its placeholder.
    assert at.session_state["kpi_airport"] is None
    assert at.session_state["kpi_reason"] is None
    reason = next(s for s in at.selectbox if s.label == "Lý do căn cứ")
    assert reason.value is None
    # Plain `st.button` Start/Submit no longer exist: they are browser-component buttons.
    assert not [b for b in at.button if b.label in ("Bắt đầu nhận ca", "Nộp & chấm điểm")]
    assert not log.exists(), "rendering must never record a trial"


def _captions(at) -> str:
    return " ".join(c.value for c in at.sidebar.caption)


def test_live_watcher_states(at, monkeypatch):
    from src.dashboard import app

    monkeypatch.setattr(app, "has_unsynced_raw_data", lambda root: False)
    at.run()
    assert not at.exception
    assert "Tự kiểm tra dữ liệu mới mỗi 60 giây" in _captions(at)
    assert not any(b.label == "Xem mốc mới" for b in at.sidebar.button)

    # Replay mode: the watcher is not shown at all.
    _replay(at, "2026-11-09T07:00:00Z")
    assert "Tự kiểm tra" not in _captions(at)

    # LIVE again with a KPI trial open: paused, nothing may change under the participant.
    at.session_state["op_mode"] = "LIVE"
    at.session_state["timer_has_started"] = True
    at.run()
    assert "tạm dừng trong lượt thử KPI" in _captions(at)
    assert not any(b.label == "Xem mốc mới" for b in at.sidebar.button)


def test_live_watcher_offers_refresh_and_jumps_to_newest_slot(at, monkeypatch):
    from src.dashboard import app, data_loader

    monkeypatch.setattr(app, "has_unsynced_raw_data", lambda root: True)
    at.run()
    assert any("Có dữ liệu mới" in i.value for i in at.sidebar.info)
    newest = max(data_loader.load_decision_slots(app.STORAGE_ROOT))  # STORAGE_ROOT = isolated copy after run
    at.session_state["slot_live"] = "2026-11-09T07:00:00Z"  # user is looking at an older slot
    at.run()
    assert at.session_state["slot_live"] == "2026-11-09T07:00:00Z", "the watcher must not change the viewed slot"
    next(b for b in at.sidebar.button if b.label == "Xem mốc mới").click().run()
    assert not at.exception
    assert at.session_state["slot_live"] == newest


def test_obs_time_preview():
    assert interpret_obs_time("07:00Z") == "07:00Z · 14:00 ICT"
    assert interpret_obs_time("090700Z") == "Ngày 09 · 07:00Z · 14:00 ICT"
    assert interpret_obs_time("14h00 ICT") == "07:00Z · 14:00 ICT"
    assert "chấp nhận cả hai" in interpret_obs_time("14:00")
    assert interpret_obs_time("abc") is None and interpret_obs_time("") is None


def test_audit_tab_with_real_data(at):
    at.run()
    _select_tab(at, 2)
    assert not at.exception
    html = _html(at)
    assert "1 · Bảo toàn dòng" in html and "BẢO TOÀN 100%" in html
    assert "4 · Thống kê 5V &amp; HDFS" in html or "4 · Thống kê 5V & HDFS" in html
    assert "12.5 MB/s" not in html


def test_audit_tab_with_empty_storage(tmp_path):
    root = tmp_path / "data"
    root.mkdir()
    at = _app(root)
    at.run()
    assert not at.exception
    _select_tab(at, 2)
    assert not at.exception
    infos = " ".join(i.value for i in at.info)
    assert "Chưa có báo cáo đối soát" in infos
    assert "12.5 MB/s" not in _html(at) + infos
