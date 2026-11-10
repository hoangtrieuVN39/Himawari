"""Hệ thống Hỗ trợ Điều phối viên Thời tiết Sân bay — Giao diện Streamlit (Mô-đun 4).

app.py only orchestrates: it loads data, renders the sidebar/header and hands a ViewContext to the
per-tab renderers in src/dashboard/views/. Loaders are referenced from this module so tests can patch them here.
"""

from __future__ import annotations

from pathlib import Path
import sys

# Ensure project root is in sys.path when launched directly via `streamlit run src/dashboard/app.py`
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st

from src.dashboard import components as c
from src.dashboard.data_loader import (
    fetch_and_update_realtime,
    get_b13_image_sequence,
    has_unsynced_raw_data,
    load_audit_and_reconciliation,
    load_decision_slots,
    normalize_timestamp_pair,
    sort_slot_rows,
    sync_latest_raw_to_decision_table,
)
from src.dashboard.kpi_evaluator import (
    append_kpi_trial,
    compute_kpi_summary,
    evaluate_kpi_submission,
    load_kpi_trials,
)
from src.dashboard.views import ViewContext, active_trial_view_mode, baseline_trial_in_progress
from src.dashboard.views.audit import render_audit
from src.dashboard.views.decision import render_decision
from src.dashboard.views.kpi import VIEW_LABELS, render_kpi
from src.dashboard.views.sidebar import SORT_OPTIONS, render_sidebar  # noqa: F401 - re-exported
from src.spark.replay_slots import load_replay_slots

STORAGE_ROOT = "data"

AIRPORT_NAMES: dict[str, str] = {
    "VVDN": "Đà Nẵng (VVDN)",
    "VVNB": "Nội Bài (VVNB)",
    "VVTS": "Tân Sơn Nhất (VVTS)",
}

TAB_LABELS = ["Quyết định", "Đo KPI", "Đối soát dữ liệu"]


def _make_tabs(labels: list[str]):
    """Creates tabs with lazy execution (only the selected tab runs) when Streamlit supports it.

    Returns (tabs, lazy). On older Streamlit without st.tabs(on_change=...), falls back to eager tabs.
    """
    try:
        return st.tabs(labels, on_change="rerun", key="main_tabs"), True
    except TypeError:
        return st.tabs(labels), False


def _tab_active(tab, lazy: bool) -> bool:
    """True if the tab's content should be computed on this run."""
    return (not lazy) or bool(getattr(tab, "open", True))


def _slot_hhmm(slot_key: str, row: dict | None) -> tuple[str, str]:
    """UTC/ICT HH:MM labels for a slot: from the row if present, else derived from the slot timestamp."""
    if row and row.get("slot_utc_hhmm") and row.get("slot_vn_hhmm"):
        return row["slot_utc_hhmm"], row["slot_vn_hhmm"]
    info = normalize_timestamp_pair(slot_key)
    return info["utc_hhmm"], info["vn_hhmm"]


def _format_metar_age(age_minutes: float | None) -> str:
    """Safely formats METAR age in minutes, returning 'N/A' for missing/None values."""
    if age_minutes is None:
        return "N/A"
    return f"{int(round(float(age_minutes)))} phút"


def init_session_state() -> None:
    """Initialize necessary Streamlit session state keys."""
    defaults = {
        "timer_state": "idle",  # idle | running | stopped
        "timer_start": 0.0,
        "timer_elapsed": 0.0,
        "timer_has_started": False,
        "last_kpi_result": None,
        "participant_id": "DISPATCHER_01",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def main() -> None:
    st.set_page_config(
        page_title="Điều phối Thời tiết Sân bay",
        layout="wide",
        page_icon="✈️",
    )
    init_session_state()
    c.inject_theme()

    storage_root = STORAGE_ROOT
    sync_error = None
    # Explicit, cached (signature-based) sync of newly ingested raw batches; reads below are read-only.
    try:
        sync_latest_raw_to_decision_table(storage_root)
    except Exception as exc:  # noqa: BLE001 - surfaced to the user
        sync_error = exc
    slots = load_decision_slots(storage_root)

    replay_slots = load_replay_slots()
    available_slots = sorted(slots.keys()) if slots else [replay_slots[0]]

    choice = render_sidebar(
        storage_root=storage_root,
        slots=slots,
        available_slots=available_slots,
        replay_slots=replay_slots,
        slot_hhmm=_slot_hhmm,
        fetch_realtime=fetch_and_update_realtime,
        has_unsynced=has_unsynced_raw_data,
    )
    selected_slot = choice.selected_slot
    rows = sort_slot_rows(slots.get(selected_slot, []), sort_mode=choice.sort_mode)

    utc, vn = _slot_hhmm(selected_slot, rows[0] if rows else None)
    d = selected_slot[:10]
    date_vn = f"{d[8:10]}/{d[5:7]}/{d[0:4]}"
    context = f"Thứ tự bảng: {choice.sort_label}"
    if st.session_state.get("timer_has_started"):
        mode = active_trial_view_mode(st.session_state)
        context = f"Lượt thử: {VIEW_LABELS.get(mode, mode)}"
    st.html(c.page_header(choice.is_live_mode, utc, vn, date_vn, context))

    if sync_error is not None:
        st.warning(f"**Đồng bộ dữ liệu thất bại.** Đang hiển thị mốc gần nhất đã lưu. Chi tiết: {sync_error}")
    if not slots:
        st.error("Không tìm thấy bảng quyết định trong `data/processed/decision_table/`. Chạy lại pipeline để tạo.")

    ctx = ViewContext(
        storage_root=storage_root,
        slots=slots,
        selected_slot=selected_slot,
        rows=rows,
        is_live_mode=choice.is_live_mode,
        latest_slot=available_slots[-1],
        sort_label=choice.sort_label,
        deps={
            "get_b13_image_sequence": get_b13_image_sequence,
            "load_audit_and_reconciliation": load_audit_and_reconciliation,
            "load_kpi_trials": load_kpi_trials,
            "append_kpi_trial": append_kpi_trial,
            "evaluate_kpi_submission": evaluate_kpi_submission,
            "compute_kpi_summary": compute_kpi_summary,
        },
    )

    (tab1, tab2, tab3), lazy_tabs = _make_tabs(TAB_LABELS)
    if _tab_active(tab1, lazy_tabs):
        with tab1:
            render_decision(ctx)
    if _tab_active(tab2, lazy_tabs):
        with tab2:
            render_kpi(ctx)
    if _tab_active(tab3, lazy_tabs):
        with tab3:
            render_audit(ctx)


__all__ = ["main", "baseline_trial_in_progress"]


if __name__ == "__main__":
    main()
