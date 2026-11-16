"""E8: the 0–60′ nowcast column/drill-down card appears only when the model passed its gate."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.dashboard import components as c
from src.dashboard.data_loader import load_nowcast
from src.dashboard.views.decision import resolve_nowcast
from tests.dashboard.lake_fixtures import decision_row, finalize_slot, write_lake_version

SLOT = "2026-11-10T08:05:00Z"
FEATURES = ["METAR hiện tại có mưa/dông ↑", "Tầm nhìn hiện tại ↓", "Giờ trong ngày (cos) ↑"]


def _nowcast_row(icao: str, p: float, gate: bool, slot: str = SLOT) -> dict:
    return {
        "review_slot_utc": slot, "airport_icao": icao, "probability": p, "threshold": 0.2,
        "predicted_positive": p >= 0.2, "model_run_id": "20261116T000000000000Z", "model_name": "lr",
        "gate_passed": gate, "top_features": FEATURES, "scored_at_utc": "2026-11-16T07:00:00Z",
        "review_date": slot[:10],
    }


def _lake(root: Path, gate: bool | None) -> None:
    rows = finalize_slot([decision_row(SLOT, "VVDN", 1), decision_row(SLOT, "VVNB", 3), decision_row(SLOT, "VVTS", 4)])
    write_lake_version(root, "decision_table", rows, run_id="20261110T080600000000Z")
    if gate is not None:
        probs = {"VVDN": 0.83, "VVNB": 0.37, "VVTS": 0.04}
        write_lake_version(root, "nowcast", [_nowcast_row(i, p, gate) for i, p in probs.items()], run_id="20261116T070000000000Z")


# --- components -----------------------------------------------------------------------------------------

def _row(icao: str, level: int) -> dict:
    return {"airport_icao": icao, "priority_level": level, "reason_vi": "Lý do", "obs_utc_hhmm": "07:00Z", "obs_vn_hhmm": "14:00",
            "metar_is_present": True, "metar_age_minutes": 5.0, "visibility_m": 9999, "forecast_rain_next_hour_mm": 0.0,
            "b13_is_present": False}


def test_airport_table_nowcast_column_only_when_requested():
    rows = [_row("VVDN", 1), _row("VVNB", 4)]
    plain = c.airport_table(rows)
    assert "Nguy cơ 0–60′" not in plain
    shown = c.airport_table(rows, nowcast={"VVDN": {"probability": 0.834, "predicted_positive": True}})
    assert "<th>Nguy cơ 0–60′</th>" in shown
    assert "83%" in shown and shown.count("chỉ tham khảo") == 1          # VVNB has no row -> dash, no tag
    assert shown.count("<tr class=") == plain.count("<tr class=")


def test_nowcast_card_lists_three_features_and_escapes():
    html = c.nowcast_card({"probability": 0.37, "threshold": 0.2, "predicted_positive": True, "top_features": FEATURES + ["<b>x</b>"]})
    assert "37" in html and "chỉ tham khảo" in html and "ngưỡng 20%" in html
    assert html.count("<li>") == 3 and "<b>" not in html
    assert c.nowcast_card(None) == "" and c.nowcast_card({"probability": None}) == ""


def test_nowcast_notes_explain_the_hidden_states():
    assert "chưa vượt các phương pháp cơ sở" in c.nowcast_note("hidden")
    assert "Chưa có mô hình" in c.nowcast_note("none")
    assert "Chưa có dự đoán" in c.nowcast_note("no_slot")


# --- loader / state resolution --------------------------------------------------------------------------

def test_load_nowcast_keys_rows_by_slot_and_airport(tmp_path: Path):
    assert load_nowcast(str(tmp_path)) == {}
    _lake(tmp_path, gate=True)
    rows = load_nowcast(str(tmp_path))
    assert set(rows) == {(SLOT, "VVDN"), (SLOT, "VVNB"), (SLOT, "VVTS")}
    row = rows[(SLOT, "VVDN")]
    assert row["probability"] == pytest.approx(0.83) and row["gate_passed"] is True
    assert list(row["top_features"]) == FEATURES


def test_resolve_nowcast_states():
    icaos = ["VVDN", "VVNB", "VVTS"]
    rows = {(SLOT, "VVDN"): _nowcast_row("VVDN", 0.8, True)}
    assert resolve_nowcast({}, SLOT, icaos) == ("none", {})
    assert resolve_nowcast({(SLOT, "VVDN"): _nowcast_row("VVDN", 0.8, False)}, SLOT, icaos) == ("hidden", {})
    state, by_icao = resolve_nowcast(rows, SLOT, icaos)
    assert state == "visible" and list(by_icao) == ["VVDN"]
    assert resolve_nowcast(rows, "2026-11-10T09:00:00Z", icaos) == ("no_slot", {})


# --- rendered tab ---------------------------------------------------------------------------------------

def _entrypoint(storage_root: str):
    from src.dashboard import app as dashboard_app

    dashboard_app.STORAGE_ROOT = storage_root
    dashboard_app.main()


def _html(at) -> str:
    return "\n".join(el.proto.body for el in at.get("html") if not el.proto.body.startswith("<style>"))


def _render(root: Path) -> str:
    at = AppTest.from_function(_entrypoint, args=(str(root),), default_timeout=120)
    at.session_state["op_mode"] = "Phát lại"
    at.session_state["slot_replay"] = SLOT
    at.run()
    assert not at.exception
    return _html(at)


def _airport_order(html: str) -> list[str]:
    table = html.split('<table class="tbl airports">')[1].split("</table>")[0]
    return re.findall(r'<span class="mono strong">(VV[A-Z]{2})</span>', table)


def test_decision_tab_shows_nowcast_only_when_gate_passed(tmp_path: Path):
    passed, failed, absent = tmp_path / "passed", tmp_path / "failed", tmp_path / "absent"
    for root, gate in ((passed, True), (failed, False), (absent, None)):
        root.mkdir()
        _lake(root, gate)

    shown = _render(passed)
    assert "<th>Nguy cơ 0–60′</th>" in shown
    assert "83%" in shown and "37%" in shown and "chỉ tham khảo" in shown
    assert "3 đặc trưng đóng góp lớn nhất" in shown and "METAR hiện tại có mưa/dông" in shown

    for root, expected in ((failed, "chưa vượt các phương pháp cơ sở"), (absent, "Chưa có mô hình")):
        hidden = _render(root)
        assert "Nguy cơ 0–60′</th>" not in hidden and "83%" not in hidden
        assert "3 đặc trưng đóng góp lớn nhất" not in hidden
        assert expected in hidden
        assert _airport_order(hidden) == _airport_order(shown) == ["VVDN", "VVNB", "VVTS"]   # Level 1–4 order unchanged
