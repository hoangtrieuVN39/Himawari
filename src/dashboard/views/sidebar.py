"""Sidebar: operating mode, review slot, table order, LIVE refresh and the technical admin area."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Callable

import streamlit as st

from src.dashboard import components as c

MODE_LIVE = "LIVE"
MODE_REPLAY = "Phát lại"

SORT_OPTIONS: dict[str, str] = {
    "Theo mức ưu tiên": "priority",
    "Mã sân bay A→Z": "icao_asc",
    "Mã sân bay Z→A": "icao_desc",
}


@dataclass
class SidebarChoice:
    is_live_mode: bool
    selected_slot: str
    sort_mode: str
    sort_label: str


def _slot_label(slot_key: str, slots: dict[str, list[dict[str, Any]]], slot_hhmm: Callable) -> str:
    rows = slots.get(slot_key) or []
    utc, vn = slot_hhmm(slot_key, rows[0] if rows else None)
    d = slot_key[:10]
    date_vn = f"{d[8:10]}/{d[5:7]}" if len(d) == 10 else d  # year is shown in the page header
    return f"{utc} ({vn} ICT) — {date_vn}"


def _live_refresh(storage_root: str, fetch: Callable[..., dict[str, Any]]) -> None:
    include_hima = st.checkbox(
        "Tải kèm ảnh vệ tinh Himawari-9 B13",
        value=True,
        help="Tải thêm phân đoạn HSD B13 mới nhất từ NOAA S3 và cắt ảnh cho 3 sân bay (~15–25 giây)",
    )
    clicked = st.button("Cập nhật dữ liệu realtime", width="stretch", type="primary")
    retry = False
    err = st.session_state.get("live_error")
    if err and not clicked:
        st.error("**Cập nhật thất bại.** Dữ liệu đang hiển thị là mốc gần nhất đã lưu.")
        with st.expander("Chi tiết lỗi"):
            st.code(str(err)[:1500], language="text")
        retry = st.button("Thử lại", width="stretch")
    msg = st.session_state.pop("live_success", None)
    if msg:
        st.success(msg)
    if not (clicked or retry):
        st.caption("Có thể mất 15 giây (LIVE) đến 3 phút (pipeline).")
        return

    with st.status("Đang cập nhật METAR, Open-Meteo & Himawari-9 B13…", expanded=True) as status:
        res = fetch(storage_root=storage_root, include_himawari=include_hima)
        if res.get("status") == "SUCCESS":
            st.session_state.pop("live_error", None)
            latest = res.get("latest_slot_utc") or ""
            status.update(label="Cập nhật thành công", state="complete")
            st.session_state["live_success"] = f"Cập nhật thành công · mốc mới {latest}"
            st.rerun()
        else:
            status.update(label="Cập nhật thất bại", state="error")
            st.session_state["live_error"] = res.get("error") or "Lỗi không xác định"
            st.error("**Cập nhật thất bại.**")
            with st.expander("Chi tiết lỗi", expanded=True):
                st.code(str(res.get("error"))[:1500], language="text")


FOLLOW_LATEST_KEY = "follow_latest_slot"
WATCH_INTERVAL_S = 60


def _watch_body(storage_root: str, has_unsynced: Callable[[str], bool]) -> None:
    """Looks for newly ingested data. Never changes the slot being viewed; only offers a refresh."""
    if has_unsynced(storage_root):
        st.info("**Có dữ liệu mới** đã được thu thập.")
        if st.button("Xem mốc mới", width="stretch", key="watch_refresh"):
            st.session_state[FOLLOW_LATEST_KEY] = True
            st.rerun(scope="app")  # full rerun: main() syncs, then the selectbox jumps to the newest slot
    else:
        st.caption(f"Tự kiểm tra dữ liệu mới mỗi {WATCH_INTERVAL_S} giây.")


def _live_watcher(storage_root: str, has_unsynced: Callable[[str], bool]) -> None:
    # Paused while a KPI trial is open (nothing may change under the participant) and outside LIVE mode.
    if st.session_state.get("timer_has_started", False):
        st.caption("Tự kiểm tra tạm dừng trong lượt thử KPI.")
        return
    st.fragment(run_every=WATCH_INTERVAL_S)(_watch_body)(storage_root, has_unsynced)


def _admin_area() -> None:
    with st.expander("Quản trị kỹ thuật", expanded=False):
        inject = st.checkbox(
            "Bơm dữ liệu lỗi kiểm thử",
            value=False,
            help="Chạy pipeline kèm --inject-quarantine-test để kiểm thử bộ lọc cách ly",
        )
        st.caption("Chỉ dùng khi demo / kiểm thử")
        if st.button("Chạy lại pipeline PySpark", width="stretch"):
            with st.status("Đang chạy pipeline PySpark…", expanded=True) as status:
                cmd = [sys.executable, "-m", "src.spark.run_pipeline"]
                if inject:
                    cmd.append("--inject-quarantine-test")
                try:
                    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
                    if proc.returncode == 0:
                        status.update(label="Pipeline hoàn thành", state="complete")
                        st.success("Đã tái tạo dữ liệu thành công.")
                        st.rerun()
                    else:
                        status.update(label="Pipeline thất bại", state="error")
                        st.error("Pipeline PySpark trả về lỗi.")
                        st.code((proc.stderr or proc.stdout or "")[-1500:], language="text")
                except Exception as exc:  # noqa: BLE001 - surfaced to the operator
                    status.update(label="Ngoại lệ hệ thống", state="error")
                    st.error(f"Ngoại lệ: {exc}")


def render_sidebar(
    storage_root: str,
    slots: dict[str, list[dict[str, Any]]],
    available_slots: list[str],
    replay_slots: list[str],
    slot_hhmm: Callable,
    fetch_realtime: Callable[..., dict[str, Any]],
    has_unsynced: Callable[[str], bool] | None = None,
) -> SidebarChoice:
    latest = available_slots[-1]
    with st.sidebar:
        st.html(c.sidebar_brand())
        st.html(c.sidebar_section("ĐIỀU PHỐI"))

        mode = st.segmented_control(
            "Chế độ vận hành",
            options=[MODE_LIVE, MODE_REPLAY],
            default=MODE_LIVE,
            key="op_mode",
            format_func=lambda m: "● LIVE" if m == MODE_LIVE else m,
        )
        is_live = (mode or MODE_LIVE) == MODE_LIVE

        if is_live:
            if st.session_state.pop(FOLLOW_LATEST_KEY, False):
                st.session_state["slot_live"] = latest  # "Xem mốc mới": jump to the newest synced slot
            selected = st.selectbox(
                "Mốc thời gian",
                options=available_slots,
                index=len(available_slots) - 1,
                format_func=lambda s: _slot_label(s, slots, slot_hhmm),
                key="slot_live",
            )
            if selected == latest:
                st.html('<span class="live-tag"><span class="dot"></span>LIVE MỚI NHẤT</span>')
            _live_refresh(storage_root, fetch_realtime)
            if has_unsynced is not None:
                _live_watcher(storage_root, has_unsynced)
        else:
            options = [s for s in replay_slots if s in slots] or available_slots
            selected = st.selectbox(
                "Mốc thời gian",
                options=options,
                index=0,
                format_func=lambda s: _slot_label(s, slots, slot_hhmm),
                key="slot_replay",
            )
            st.caption("Mốc cố định cho bài thử KPI · chạy offline, chuyển mốc < 1 giây")

        sort_label = st.selectbox("Thứ tự hiển thị bảng", options=list(SORT_OPTIONS), index=0, key="sort_mode")
        st.caption("Đổi thứ tự dòng không làm đổi đáp án.")

        _admin_area()
        st.html(c.sidebar_footer())

    return SidebarChoice(is_live_mode=is_live, selected_slot=selected, sort_mode=SORT_OPTIONS[sort_label], sort_label=sort_label)
