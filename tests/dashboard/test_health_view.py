"""Health tab renders from Gold artifacts (streamlit AppTest) without computing on render."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from streamlit.testing.v1 import AppTest

from src.dashboard.views import health as health_view
from src.diagnostics.run_health import run_health

UTC = timezone.utc


def _entrypoint(storage_root: str):
    from src.dashboard import components as c
    from src.dashboard.views.health import render_health_tab

    c.inject_theme()
    render_health_tab(storage_root)


def _app(root: Path) -> AppTest:
    return AppTest.from_function(_entrypoint, args=(str(root),), default_timeout=120)


def _html(at) -> str:
    return "\n".join(el.proto.body for el in at.get("html") if not el.proto.body.startswith("<style>"))


def _fixture(root: Path) -> None:
    now = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)
    iso = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    logs = root / "logs"
    logs.mkdir(parents=True)
    lines = [
        {"source": "metar", "airport": "ALL", "fetched_at_utc": iso(now - timedelta(minutes=10 * i)),
         "expected_slot_utc": iso(now), "status": "SUCCESS", "record_count": 12, "bytes_written": 1000 + 7 * i, "is_backfill": False}
        for i in range(12)
    ]
    (logs / "ingestion_log.jsonl").write_text("\n".join(json.dumps(x) for x in lines) + "\n", encoding="utf-8")
    (logs / "pipeline_runs.jsonl").write_text(json.dumps({
        "completed_at_utc": iso(now - timedelta(minutes=20)), "latency_s": 31.5,
        "timings_s": {"session_init": 3.0, "total": 12.0}}) + "\n", encoding="utf-8")
    run_health(str(root), 24, False, now=now)


def test_renders_without_summary_shows_hint_and_button(tmp_path):
    at = _app(tmp_path / "data").run()
    assert not at.exception
    assert [b.label for b in at.button] == ["Tính lại chỉ số sức khỏe"]
    assert any("Chưa có chỉ số sức khỏe" in i.value for i in at.info)


def test_renders_all_sections_on_fixture(tmp_path):
    root = tmp_path / "data"
    _fixture(root)
    at = _app(root).run()
    assert not at.exception
    html = _html(at)
    for title in ("Sức khỏe hệ thống", "1 · Độ mới nguồn (SLA)", "2 · Độ phủ Himawari theo ngày",
                  "3 · Nguyên nhân khoảng thiếu", "4 · KPI bảng quyết định", "5 · Bất thường",
                  "6 · Trôi dữ liệu", "7 · Hồ sơ dữ liệu theo nguồn", "8 · Chi phí"):
        assert title in html, title
    assert "METAR (AWC)" in html and "31.5 s" in html and "Giai đoạn" in html


def test_recompute_button_runs_cli_function(tmp_path, monkeypatch):
    root = tmp_path / "data"
    _fixture(root)
    calls = []
    import src.diagnostics.run_health as rh

    original = rh.run_health

    def spy(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(rh, "run_health", spy)
    at = _app(root).run()
    at.button[0].click().run()
    assert not at.exception
    assert calls and calls[0][0] == str(root) and calls[0][2] is False
    assert any("Đã cập nhật" in s.value for s in at.success)


def test_formatting_helpers():
    assert health_view.fmt_value({"value": 0.5, "unit": "ratio"}) == "50.0%"
    assert health_view.fmt_value({"value": None, "status": "not_available"}) == "không khả dụng"
    assert health_view.fmt_value({"value": 55.0, "unit": "seconds"}) == "55.0 s"
    assert health_view.fmt_hours([]) == "—"
    assert health_view.fmt_hours(["2026-11-16T03:00:00Z"] * 6, limit=2).endswith("(+4)")
    assert health_view.load_summary("does/not/exist") is None
