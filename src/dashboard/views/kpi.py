"""Tab "Đo KPI": Baseline vs Dashboard trial views, live timer, submission form and KPI summary."""

from __future__ import annotations

import re
import time
from typing import Any

import pandas as pd
import streamlit as st

from src.dashboard import components as c
from src.dashboard.views import (
    KPI_VIEW_DEFAULT,
    KPI_VIEW_MODE_KEY,
    TRIAL_VIEW_MODE_KEY,
    ViewContext,
    active_trial_view_mode,
)

VIEW_LABELS = {"baseline_3_sources": "Baseline: 3 nguồn rời", "dashboard": "Dashboard tổng hợp"}

REASON_LABELS: dict[str, str] = {
    "PRECIP_OR_THUNDER_AT_STATION": "Mưa/dông tại trạm",
    "LOW_VISIBILITY": "Tầm nhìn thấp (< 5000m)",
    "STALE_OR_MISSING_METAR": "METAR thiếu/quá cũ (> 60 phút)",
    "FORECAST_RAIN_NEXT_HOUR": "Dự báo mưa giờ tới (≥ 0.1 mm/h)",
    "ROUTINE": "Bình thường",
}

NOT_STARTED_MSG = "Chưa bắt đầu lượt thử. Hãy bấm “Bắt đầu nhận ca” trước khi nộp."


def interpret_obs_time(text: str | None) -> str | None:
    """Explains how an entered observation time will be read ("Hiểu là: …"); None when unparseable.

    Mirrors the formats accepted by kpi_evaluator.matches_observation_time.
    """
    if not text or not text.strip():
        return None
    s = text.strip().upper()

    def utc_to_ict(h: int, m: int) -> str:
        return f"{(h + 7) % 24:02d}:{m:02d}"

    def ict_to_utc(h: int, m: int) -> str:
        return f"{(h - 7) % 24:02d}:{m:02d}Z"

    m6 = re.match(r"^(\d{2})(\d{2})(\d{2})\s*(Z|UTC)?$", s)
    if m6:
        d, h, mi = (int(x) for x in m6.groups()[:3])
        if 1 <= d <= 31 and h <= 23 and mi <= 59:
            return f"Ngày {d:02d} · {h:02d}:{mi:02d}Z · {utc_to_ict(h, mi)} ICT"
        return None
    m = (
        re.match(r"^(\d{1,2}):(\d{2})\s*(Z|UTC|ICT|VN)?$", s)
        or re.match(r"^(\d{2})(\d{2})\s*(Z|UTC|ICT|VN)?$", s)
        or re.match(r"^(\d{1,2})H(\d{2})\s*(Z|UTC|ICT|VN)?$", s)
    )
    if m:
        h, mi, tz = int(m.group(1)), int(m.group(2)), m.group(3)
    else:
        mh = re.match(r"^(\d{1,2})\s*H?\s*(Z|UTC|ICT|VN)?$", s)
        if not mh:
            return None
        h, mi, tz = int(mh.group(1)), 0, mh.group(2)
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        return None
    if tz in ("Z", "UTC"):
        return f"{h:02d}:{mi:02d}Z · {utc_to_ict(h, mi)} ICT"
    if tz in ("ICT", "VN"):
        return f"{ict_to_utc(h, mi)} · {h:02d}:{mi:02d} ICT"
    return f"{h:02d}:{mi:02d}Z (UTC) hoặc {h:02d}:{mi:02d} ICT — chấp nhận cả hai"


def _elapsed_now() -> float:
    if st.session_state["timer_state"] == "running":
        return round(max(0.0, time.perf_counter() - st.session_state["timer_start"]), 1)
    return float(st.session_state["timer_elapsed"])


def _baseline_sources(ctx: ViewContext) -> None:
    st.warning("**Đang ở chế độ Baseline: 3 nguồn rời.** Gợi ý ưu tiên đã bị ẩn — người thử tự tổng hợp từ 3 nguồn.")
    rows = ctx.rows
    b1, b2, b3 = st.columns(3)
    with b1:
        st.markdown("**1 · METAR thô (AviationWeather)**")
        metar_lines = "\n\n".join(f"{r['airport_icao']}:\n{r.get('raw_ob') or 'N/A'}" for r in rows)
        st.text_area("METAR", value=metar_lines, height=200, disabled=True, label_visibility="collapsed")
    with b2:
        st.markdown("**2 · Dự báo số liệu thô (Open-Meteo)**")
        st.html(c.data_table(
            ["ICAO", "Mưa (mm/h)", "Thời điểm nhận"],
            [[r["airport_icao"], r.get("forecast_rain_next_hour_mm", 0.0) or 0.0, r.get("forecast_fetched_at_utc") or "Lịch sử (0.0)"] for r in rows],
            mono_cols=(0, 1, 2),
        ))
    with b3:
        st.markdown("**3 · Kho ảnh vệ tinh Himawari-9 B13**")
        inspect = st.selectbox("Xem ảnh sân bay", [r["airport_icao"] for r in rows], key="base_img_sel")
        seq = ctx.deps["get_b13_image_sequence"](ctx.storage_root, inspect, ctx.selected_slot, max_frames=1)
        from pathlib import Path

        if seq and Path(seq[-1]["png_path"]).exists():
            st.image(seq[-1]["png_path"], caption=f"Himawari-9 {inspect} B13", width="stretch")
        else:
            st.caption("Chưa có ảnh vệ tinh cho sân bay này.")


def _clear_answers() -> None:
    """Every trial starts from an empty form (no answer carried over from the previous trial).

    Must run before the form widgets are created in this script run (the timer block renders first).
    """
    st.session_state["kpi_airport"] = None
    st.session_state["kpi_reason"] = None
    st.session_state["kpi_obs_time"] = ""


def _timer_block(view_mode: str) -> None:
    st.html('<div class="card-title">Bấm giờ</div>')
    running = st.session_state["timer_state"] == "running"

    @st.fragment(run_every=1.0 if running else None)
    def _clock() -> None:
        st.html(c.timer_display(st.session_state["timer_state"], _elapsed_now()))

    _clock()

    if st.button("Bắt đầu nhận ca", width="stretch", type="primary"):
        st.session_state["timer_state"] = "running"
        st.session_state["timer_start"] = time.perf_counter()
        st.session_state["timer_elapsed"] = 0.0
        st.session_state["timer_has_started"] = True
        st.session_state["last_kpi_result"] = None
        st.session_state[TRIAL_VIEW_MODE_KEY] = view_mode
        _clear_answers()
        st.rerun()
    b2, b3 = st.columns(2)
    with b2:
        if st.button("Dừng & ghi nhận", width="stretch"):
            if st.session_state["timer_state"] == "running":
                st.session_state["timer_state"] = "stopped"
                st.session_state["timer_elapsed"] = round(max(0.0, time.perf_counter() - st.session_state["timer_start"]), 1)
                st.rerun()
    with b3:
        if st.button("Đặt lại", width="stretch"):
            st.session_state["timer_state"] = "idle"
            st.session_state["timer_start"] = 0.0
            st.session_state["timer_elapsed"] = 0.0
            st.session_state["timer_has_started"] = False
            st.session_state["last_kpi_result"] = None
            st.session_state.pop(TRIAL_VIEW_MODE_KEY, None)
            _clear_answers()
            st.rerun()
    st.caption("Đồng hồ chạy liên tục trên màn hình. Nộp bài sẽ tự dừng; lượt sau cần bấm “Bắt đầu nhận ca” lại.")


def _failure_lines(res: dict[str, Any]) -> list[str]:
    lines = []
    codes = res.get("failure_codes", [])
    if "WRONG_AIRPORT" in codes:
        lines.append(f"Sai sân bay: đã chọn {res.get('selected_airport')}")
    if "UNKNOWN_AIRPORT" in codes:
        lines.append(f"Không có dữ liệu cho sân bay {res.get('selected_airport')}")
    if "WRONG_REASON" in codes:
        lines.append(f"Sai lý do: đã chọn “{REASON_LABELS.get(res.get('selected_reason_code'), res.get('selected_reason_code'))}”")
    if "WRONG_OBS_TIME" in codes:
        lines.append(f"Sai giờ bản tin: {res.get('entered_obs_time') or '(để trống)'}")
    if "ABANDONED" in codes:
        lines.append("Lượt thử bị bỏ dở")
    return lines


def _result_banner() -> None:
    res = st.session_state.get("last_kpi_result")
    if not res:
        return
    secs = float(res.get("elapsed_seconds", st.session_state.get("timer_elapsed", 0.0)))
    if res["is_passed"]:
        st.success(f"**ĐẠT — {secs:.1f} giây**  \nĐúng sân bay, đúng lý do và đúng giờ bản tin.")
        return
    lines = _failure_lines(res)
    bullets = "\n".join(f"- {line}" for line in lines)
    st.error(f"**CHƯA ĐẠT — {secs:.1f} giây · {len(lines)} lỗi**\n\n{bullets}")
    accepted = ", ".join(res.get("accepted_airports") or [])
    st.info(
        "**ĐÁP ÁN ĐÚNG**  \n"
        f"Sân bay: {accepted} · Giờ: {res.get('expected_obs_time_utc')} ({res.get('expected_obs_time_vn')} ICT) · "
        f"Lý do: {REASON_LABELS.get(res.get('expected_reason_code'), res.get('expected_reason_code'))}"
    )


def _submission_form(ctx: ViewContext, view_mode: str) -> None:
    st.html('<div class="card-title">Phiếu nộp kết quả</div>')
    f1, f2 = st.columns(2)
    with f1:
        participant_id = st.text_input("Mã người thử", value=st.session_state["participant_id"])
        st.session_state["participant_id"] = participant_id
    with f2:
        trial_order = st.segmented_control(
            "Thứ tự lượt thử", options=[1, 2], default=1, key="kpi_trial_order", format_func=lambda n: f"Lượt {n}",
        ) or 1

    airports = ["VVDN", "VVNB", "VVTS"]
    selected_airport = st.segmented_control(
        "Sân bay cần rà soát đầu tiên",
        options=airports,
        key="kpi_airport",
        format_func=lambda code: f"{code} · {c.AIRPORT_SHORT_NAMES.get(code, code)}",
    )
    selected_reason = st.selectbox(
        "Lý do căn cứ",
        options=[None, *REASON_LABELS.keys()],
        format_func=lambda k: "— Chọn lý do —" if k is None else REASON_LABELS[k],
        key="kpi_reason",
    )
    entered_obs_time = st.text_input(
        "Giờ bản tin làm căn cứ", placeholder="Ví dụ: 07:00Z hoặc 14:00", key="kpi_obs_time",
    )
    hint = interpret_obs_time(entered_obs_time)
    if entered_obs_time and hint:
        st.caption(f"Hiểu là: **{hint}**")
    elif entered_obs_time:
        st.caption("⚠ Không đọc được định dạng giờ này.")
    st.caption("Nhận: 07:00Z · 0700Z · 090700Z (UTC) hoặc 14:00 · 14h00 (ICT)")
    if not st.session_state.get("timer_has_started", False):
        st.caption("Phải bấm “Bắt đầu nhận ca” trước khi nộp")

    if not st.button("Nộp & chấm điểm", width="stretch", type="primary"):
        return
    if not st.session_state.get("timer_has_started", False):
        st.warning(NOT_STARTED_MSG)
        return
    if not selected_airport:
        st.warning("Chọn sân bay cần rà soát đầu tiên trước khi nộp.")
        return
    if not selected_reason:
        st.warning("Chọn lý do căn cứ trước khi nộp.")
        return

    if st.session_state["timer_state"] == "running":
        final_elapsed = round(max(0.0, time.perf_counter() - st.session_state["timer_start"]), 1)
        st.session_state["timer_state"] = "stopped"
        st.session_state["timer_elapsed"] = final_elapsed
    else:
        final_elapsed = st.session_state["timer_elapsed"]

    eval_res = ctx.deps["evaluate_kpi_submission"](
        slot_rows=ctx.rows,
        selected_airport=selected_airport,
        selected_reason_code=selected_reason,
        entered_obs_time=entered_obs_time,
        elapsed_seconds=final_elapsed,
        status="COMPLETED",
    )
    ctx.deps["append_kpi_trial"]({
        "participant_id": participant_id,
        "case_id": ctx.selected_slot,
        "view_mode": view_mode,
        "trial_order": trial_order,
        "selected_airport": selected_airport,
        "selected_reason_code": selected_reason,
        "entered_obs_time": entered_obs_time,
        "elapsed_seconds": final_elapsed,
        "status": "COMPLETED",
        "is_passed": eval_res["is_passed"],
        "failure_codes": eval_res["failure_codes"],
        "failure_message_vi": eval_res["failure_message_vi"],
        "is_sample_seed": False,
    })
    st.session_state["last_kpi_result"] = {
        **eval_res,
        "elapsed_seconds": final_elapsed,
        "selected_airport": selected_airport,
        "selected_reason_code": selected_reason,
        "entered_obs_time": entered_obs_time,
    }
    # Re-arm: a new trial needs a fresh "Bắt đầu nhận ca" (prevents duplicate records)
    st.session_state["timer_has_started"] = False
    st.session_state.pop(TRIAL_VIEW_MODE_KEY, None)
    st.rerun()  # full rerun so the clock fragment stops ticking and the mode switch unlocks


def render_kpi(ctx: ViewContext) -> None:
    st.html(c.section_header(
        "Bài thử quyết định có bấm giờ",
        "Mục tiêu: Dashboard giúp quyết định nhanh hơn ít nhất 30% so với tự đọc 3 nguồn rời.",
    ))
    if KPI_VIEW_MODE_KEY not in st.session_state:
        st.session_state[KPI_VIEW_MODE_KEY] = KPI_VIEW_DEFAULT
    trial_open = bool(st.session_state.get("timer_has_started", False))
    view_mode = st.segmented_control(
        "Chế độ thử nghiệm",
        options=list(VIEW_LABELS),
        key=KPI_VIEW_MODE_KEY,
        format_func=lambda k: VIEW_LABELS[k],
        label_visibility="collapsed",
        disabled=trial_open,
    ) or KPI_VIEW_DEFAULT
    if trial_open:
        # The mode is frozen for the whole trial; switching would leak answers into a Baseline trial.
        view_mode = active_trial_view_mode(st.session_state)
        st.caption("Đang trong lượt thử — chế độ xem bị khóa. Nộp bài hoặc bấm “Đặt lại” để đổi chế độ.")

    if view_mode == "baseline_3_sources":
        _baseline_sources(ctx)
    else:
        st.html(c.mini_decision_table(ctx.rows))

    left, right = st.columns([1, 1.6], gap="medium")
    with left, st.container(border=True):
        _timer_block(view_mode)
    with right, st.container(border=True):
        _submission_form(ctx, view_mode)
    _result_banner()

    st.html(c.section_header("Thống kê KPI tổng hợp", "So sánh lượt đạt giữa Baseline và Dashboard"))
    trials = ctx.deps["load_kpi_trials"](include_seeds=True)
    summary = ctx.deps["compute_kpi_summary"](trials)
    st.html(c.kpi_summary_block(summary))
    if trials:
        with st.expander(f"Nhật ký lượt thử ({len(trials)})"):
            cols = ["participant_id", "case_id", "view_mode", "trial_order", "selected_airport", "elapsed_seconds", "is_passed", "is_sample_seed"]
            st.dataframe(pd.DataFrame(trials).reindex(columns=cols), width="stretch", hide_index=True)
