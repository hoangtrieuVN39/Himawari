"""Hệ thống Hỗ trợ Điều phối viên Thời tiết Sân bay — Giao diện Streamlit (Mô-đun 4)."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

# Ensure project root is in sys.path when launched directly via `streamlit run src/dashboard/app.py`
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd
import streamlit as st

from src.dashboard.data_loader import (
    fetch_and_update_realtime,
    get_b13_image_sequence,
    load_audit_and_reconciliation,
    load_decision_slots,
    normalize_timestamp_pair,
    sort_slot_rows,
    sync_latest_raw_to_decision_table,
)
from src.spark.replay_slots import load_replay_slots
from src.dashboard.kpi_evaluator import (
    REASON_OPTIONS,
    append_kpi_trial,
    compute_kpi_summary,
    evaluate_kpi_submission,
    load_kpi_trials,
)

STORAGE_ROOT = "data"

AIRPORT_NAMES: dict[str, str] = {
    "VVDN": "Đà Nẵng (VVDN)",
    "VVNB": "Nội Bài (VVNB)",
    "VVTS": "Tân Sơn Nhất (VVTS)",
}

SORT_OPTIONS: dict[str, str] = {
    "Mức ưu tiên (Mặc định)": "priority",
    "Mã sân bay A → Z": "icao_asc",
    "Đảo thứ tự dòng Z → A (Kiểm thử đồng mức)": "icao_desc",
}


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
    if "timer_state" not in st.session_state:
        st.session_state["timer_state"] = "idle"  # idle | running | stopped
    if "timer_start" not in st.session_state:
        st.session_state["timer_start"] = 0.0
    if "timer_elapsed" not in st.session_state:
        st.session_state["timer_elapsed"] = 0.0
    if "timer_has_started" not in st.session_state:
        st.session_state["timer_has_started"] = False
    if "last_kpi_result" not in st.session_state:
        st.session_state["last_kpi_result"] = None
    if "participant_id" not in st.session_state:
        st.session_state["participant_id"] = "DISPATCHER_01"


def main() -> None:
    st.set_page_config(
        page_title="Hệ thống Hỗ trợ Điều phối viên Thời tiết Sân bay",
        layout="wide",
        page_icon="✈️",
    )
    init_session_state()

    # --- SIDEBAR CONFIGURATION ---
    st.sidebar.title("✈️ ĐIỀU PHỐI THỜI TIẾT")
    st.sidebar.markdown("**Hệ thống Hỗ trợ Ra Quyết định & Đối soát Chất lượng Dữ liệu**")
    st.sidebar.caption("Đồ án Big Data · Mô-đun 4 · 3 Sân bay: VVDN, VVNB, VVTS")
    st.sidebar.markdown("---")

    storage_root = STORAGE_ROOT
    # Explicit, cached (signature-based) sync of newly ingested raw batches; reads below are read-only.
    try:
        sync_latest_raw_to_decision_table(storage_root)
    except Exception as exc:
        st.sidebar.warning(f"⚠️ Không đồng bộ được mốc mới nhất từ dữ liệu thô: {exc}")
    slots = load_decision_slots(storage_root)

    if not slots:
        st.sidebar.error("⚠️ Không tìm thấy bảng quyết định trong `data/processed/decision_table/`.")
        available_slots = [load_replay_slots()[0]]
    else:
        available_slots = sorted(slots.keys())

    latest_slot_key = available_slots[-1]

    # Format review slot labels showing both UTC and VN time + LIVE badge for newest slot
    def format_slot(slot_key: str) -> str:
        if slots and slot_key in slots and slots[slot_key]:
            first = slots[slot_key][0]
            badge = " 🟢 [LIVE MỚI NHẤT]" if slot_key == latest_slot_key and len(available_slots) > 2 else ""
            utc_hhmm, vn_hhmm = _slot_hhmm(slot_key, first)
            return f"{utc_hhmm} ({vn_hhmm} ICT) — {slot_key[:10]}{badge}"
        return slot_key

    operating_mode = st.sidebar.radio(
        "Chế độ vận hành:",
        options=[
            "🟢 Trực tiếp Thời gian thực (LIVE Realtime)",
            "⏪ Phát lại Lịch sử (Historical Replay)",
        ],
        index=0,
        help="Chế độ LIVE hiển thị bản tin thời tiết thực tế mới nhất vừa thu thập; Chế độ Phát lại phục vụ kiểm thử KPI ở các mốc chuẩn (07:00Z, 08:00Z).",
    )
    is_live_mode = operating_mode.startswith("🟢")

    if is_live_mode:
        selected_slot = st.sidebar.selectbox(
            "Mốc thời gian (Đang chọn LIVE mới nhất):",
            options=available_slots,
            index=len(available_slots) - 1,
            format_func=format_slot,
            help="Mặc định chọn mốc thời gian thực mới nhất đã đồng bộ từ API & vệ tinh",
        )
        include_hima_live = st.sidebar.checkbox(
            "Tải kèm ảnh vệ tinh Himawari-9 B13 mới nhất từ S3",
            value=True,
            help="Khi bật, hệ thống sẽ tải thêm phân đoạn HSD B13 mới nhất từ NOAA S3 và cắt ảnh PNG cho 3 sân bay (~15–25s)",
        )
        if st.sidebar.button("🔄 Cập nhật Dữ liệu Realtime Ngay", use_container_width=True, type="primary"):
            with st.sidebar.status("Đang thu thập METAR, Open-Meteo & Himawari-9 B13 trực tiếp...", expanded=True) as live_status:
                res = fetch_and_update_realtime(storage_root=storage_root, include_himawari=include_hima_live)
                if res.get("status") == "SUCCESS":
                    live_status.update(
                        label=f"✅ Đã cập nhật mốc LIVE {res.get('latest_slot_utc', '')}!",
                        state="complete",
                    )
                    st.sidebar.success(f"Đã đồng bộ mốc mới nhất: {res.get('latest_slot_utc')}")
                    st.rerun()
                else:
                    live_status.update(label="❌ Lỗi khi thu thập dữ liệu trực tiếp", state="error")
                    st.sidebar.error(f"Chi tiết: {res.get('error')}")
    else:
        selected_slot = st.sidebar.selectbox(
            "Mốc rà soát phát lại (Replay Slot):",
            options=available_slots,
            index=0,
            format_func=format_slot,
            help="Chọn thời điểm phát lại lịch sử (07:00Z hoặc 08:00Z) để rà soát thời tiết tại 3 sân bay",
        )

    sort_mode_label = st.sidebar.selectbox(
        "Thứ tự hiển thị bảng:",
        options=list(SORT_OPTIONS.keys()),
        index=0,
        help="Thay đổi thứ tự hiển thị dòng để kiểm chứng tính bất biến khi có đồng mức ưu tiên",
    )
    sort_mode = SORT_OPTIONS[sort_mode_label]

    st.sidebar.markdown("---")
    st.sidebar.subheader("⚙️ Vận hành PySpark Pipeline")
    inject_quarantine = st.sidebar.checkbox(
        "Bơm dữ liệu lỗi kiểm thử (--inject-quarantine-test)",
        value=False,
        help="Bật cờ để kiểm thử bộ lọc kiểm soát lỗi và cách ly dữ liệu hỏng",
    )

    if st.sidebar.button("🚀 Kích hoạt lại Pipeline PySpark", use_container_width=True):
        with st.sidebar.status("Đang chạy PySpark Pipeline...", expanded=True) as status:
            cmd = [sys.executable, "-m", "src.spark.run_pipeline"]
            if inject_quarantine:
                cmd.append("--inject-quarantine-test")
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
                if proc.returncode == 0:
                    status.update(label="✅ PySpark Pipeline hoàn thành!", state="complete")
                    st.sidebar.success("Đã tái tạo dữ liệu thành công.")
                    st.rerun()
                else:
                    status.update(label="❌ Lỗi thực thi PySpark", state="error")
                    st.sidebar.error(f"Lỗi: {proc.stderr[:300]}")
            except Exception as e:
                status.update(label="❌ Ngoại lệ hệ thống", state="error")
                st.sidebar.error(f"Ngoại lệ: {e}")

    st.sidebar.markdown("---")
    st.sidebar.caption("⚡ Hỗ trợ cả LIVE Realtime (API + S3) & Phát lại Lịch sử (< 1s)")

    # --- MAIN CONTENT AREA ---
    slot_rows = slots.get(selected_slot, [])
    sorted_rows = sort_slot_rows(slot_rows, sort_mode=sort_mode)

    slot_utc_display, slot_vn_display = _slot_hhmm(selected_slot, sorted_rows[0] if sorted_rows else None)
    slot_date_display = selected_slot[:10] if selected_slot else load_replay_slots()[0][:10]
    mode_badge = "🟢 LIVE REALTIME" if selected_slot == latest_slot_key and len(available_slots) > 2 else "⏪ REPLAY LỊCH SỬ"

    st.title("🛫 Hệ thống Hỗ trợ Điều phối viên Rà soát Thời tiết Sân bay")
    st.caption(
        f"Trạng thái: **{mode_badge}** | Mốc thời gian rà soát đang chọn: **{slot_utc_display}** "
        f"(Tương đương **{slot_vn_display} ICT** · Ngày {slot_date_display}) "
        f"| Thứ tự bảng: **{sort_mode_label}**"
    )

    (tab1, tab2, tab3), lazy_tabs = _make_tabs([
        "🎯 Bảng Rà soát Quyết định & Truy nguyên Bằng chứng",
        "⏱️ Đo lường & Chấm điểm Tự động KPI (CLO4)",
        "🛡️ Đối soát Dữ liệu, Quarantine & Ma trận Lưu trữ (CLO2/CLO3)",
    ])

    # =========================================================================
    # TAB 1: BẢNG RÀ SOÁT QUYẾT ĐỊNH & TRUY NGUYÊN BẰNG CHỨNG
    # =========================================================================
    if _tab_active(tab1, lazy_tabs):
        with tab1:
            st.subheader("🎯 Bảng Tổng hợp Rà soát Quyết định")

            if not sorted_rows:
                st.warning("Không có dữ liệu dòng cho mốc thời gian này.")
            else:
                first_row = sorted_rows[0]
                accepted_airports = first_row.get("accepted_first_review_airports", [])
                co_priority_count = first_row.get("co_priority_top_count", len(accepted_airports))

                # Banner quyết định
                if co_priority_count > 1:
                    st.warning(
                        f"⚠️ **ĐỒNG MỨC ƯU TIÊN ĐẦU (Co-Priority Tie) — Điều phối viên có thể chọn bất kỳ sân bay nào "
                        f"trong nhóm và dẫn căn cứ riêng:** {', '.join(accepted_airports)} "
                        f"(Cả hai sân bay đều ghi nhận hiện tượng nguy hiểm mức 1)."
                    )
                else:
                    st.info(
                        f"🎯 **Sân bay cần rà soát đầu tiên (Mức ưu tiên cao nhất):** {', '.join(accepted_airports)} "
                        f"— {first_row.get('reason_vi', '')}"
                    )

                # Table of 3 airports
                table_display_data: list[dict[str, Any]] = []
                for r in sorted_rows:
                    icao = r.get("airport_icao", "")
                    p_level = r.get("priority_level", 4)
                    name = AIRPORT_NAMES.get(icao, icao)
                    vis = r.get("visibility_m")
                    vis_str = f"{vis}m" if vis is not None else "N/A"
                    if r.get("visibility_capped_9999"):
                        vis_str += " (≥9999)"
                    rain = r.get("forecast_rain_next_hour_mm")
                    rain_str = f"{rain:.1f} mm/h" if rain is not None else "0.0 mm/h"
                    bt_min = r.get("inner_bt_min_c")
                    bt_str = f"{bt_min:.1f}°C" if bt_min is not None else "N/A"
                    b13_summary = r.get("b13_summary_vi", "Bình thường")

                    table_display_data.append({
                        "Mức ưu tiên": f"Mức {p_level}",
                        "Mã sân bay": icao,
                        "Tên sân bay": name,
                        "Lý do phân loại": r.get("reason_vi", ""),
                        "Giờ quan trắc": f"{r.get('obs_utc_hhmm', '')} ({r.get('obs_vn_hhmm', '')} ICT)",
                        "Tuổi METAR": _format_metar_age(r.get("metar_age_minutes")),
                        "Tầm nhìn": vis_str,
                        "Mưa giờ tới (Dự báo)": rain_str,
                        "Mây B13 (Vệ tinh)": f"{b13_summary} (BT min: {bt_str})",
                        "Trạng thái nguồn": "✅ Đầy đủ 3 nguồn" if r.get("b13_is_present") and r.get("metar_is_present") else "⚠️ Khuyết nguồn",
                    })

                df_summary = pd.DataFrame(table_display_data)
                st.dataframe(df_summary, use_container_width=True, hide_index=True)

                st.markdown("---")
                st.subheader("🔍 Truy nguyên Bằng chứng Chi tiết (Provenance Drill-Down)")

                drill_icao_list = [r["airport_icao"] for r in sorted_rows]
                drill_icao = st.selectbox(
                    "Chọn sân bay để xem toàn bộ bằng chứng gốc từ 3 nguồn:",
                    options=drill_icao_list,
                    format_func=lambda code: f"{code} — {AIRPORT_NAMES.get(code, code)}",
                )

                drill_row = next((r for r in sorted_rows if r.get("airport_icao") == drill_icao), sorted_rows[0])

                col_left, col_right = st.columns([1, 1])

                # Left column: METAR & Open-Meteo
                with col_left:
                    st.markdown("#### 1. Bản tin Quan trắc METAR Gốc")
                    raw_ob = drill_row.get("raw_ob") or "Không có bản tin METAR"
                    st.code(raw_ob, language="text")

                    c1, c2 = st.columns(2)
                    with c1:
                        st.metric("Giờ quan trắc", f"{drill_row.get('obs_utc_hhmm')} ({drill_row.get('obs_vn_hhmm')} ICT)")
                        st.metric("Tuổi dữ liệu", _format_metar_age(drill_row.get("metar_age_minutes")))
                        st.write(f"**Thời tiết tại trạm:** `{drill_row.get('station_weather_codes') or 'None'}`")
                    with c2:
                        st.metric("Tầm nhìn", f"{drill_row.get('visibility_m', 'N/A')} m")
                        st.metric("Giờ nhận tin", f"{drill_row.get('metar_fetched_at_utc', 'N/A')}")
                        st.write(f"**Thời tiết lân cận:** `{drill_row.get('vicinity_weather_codes') or 'None'}`")

                    st.write(f"**Xu hướng (TEMPO/BECMG):** `{drill_row.get('trend_weather_codes') or 'None'}`")
                    st.write(f"**Phiên bản sửa đổi / SPECI:** Số hiệu revision `{drill_row.get('revision_index', 0)}` (Đã sửa/SPECI: `{drill_row.get('is_amended_or_speci', False)}`)")

                    st.markdown("#### 2. Mô hình Dự báo Open-Meteo")
                    rain_val = drill_row.get("forecast_rain_next_hour_mm")
                    rain_val_float = float(rain_val) if rain_val is not None else 0.0
                    fc_fetched = drill_row.get("forecast_fetched_at_utc")
                    st.metric("Lượng mưa dự báo giờ tới", f"{rain_val_float:.2f} mm/h")
                    if fc_fetched:
                        st.write(f"**Thời điểm nhận dự báo:** `{fc_fetched}`")
                    else:
                        st.caption("ℹ️ *Ghi chú: Ca phát lại lịch sử trước thời điểm bật máy thu dự báo trực tiếp. Trường dự báo mưa mặc định 0.0 mm/h theo nguyên tắc bảo thủ.*")

                # Right column: Himawari-9 B13 Satellite
                with col_right:
                    st.markdown("#### 3. Ảnh Vệ tinh Himawari-9 Kênh B13 (Hồng ngoại Sạch)")
                    b13_seq = get_b13_image_sequence(storage_root, drill_icao, selected_slot, max_frames=6)

                    if b13_seq:
                        st.write(f"**Chuỗi {len(b13_seq)} ảnh trước sự kiện (Time-Lapse 10 phút/ảnh):**")
                        selected_frame = st.select_slider(
                            "Thời điểm ảnh vệ tinh:",
                            options=b13_seq,
                            value=b13_seq[-1],
                            format_func=lambda s: f"{s['utc_hhmm']} ({s['vn_hhmm']} ICT)",
                            key="b13_slider",
                        )
                        img_path = selected_frame["png_path"]
                        if Path(img_path).exists():
                            st.image(
                                img_path,
                                caption=f"Himawari-9 B13 tại {drill_icao} lúc {selected_frame['utc_hhmm']} ({selected_frame['vn_hhmm']} ICT)",
                                use_container_width=True,
                            )
                        else:
                            st.warning(f"Tệp ảnh không tồn tại: {img_path}")
                    else:
                        latest_png = drill_row.get("latest_b13_png_path")
                        if latest_png and Path(latest_png).exists():
                            st.image(
                                latest_png,
                                caption=f"Ảnh vệ tinh Himawari-9 B13 tại {drill_icao}",
                                use_container_width=True,
                            )
                        else:
                            st.info("Chưa có ảnh vệ tinh PNG trong kho lưu trữ cho sân bay này.")

                    # Dual-box metrics
                    st.markdown("**Bộ chỉ số Đối lưu & Mây Lạnh (Dual-Box $\\pm 1.0^\\circ$ & $\\pm 0.25^\\circ$):**")
                    valid_frac = drill_row.get("b13_valid_fraction")
                    valid_frac_val = float(valid_frac) if valid_frac is not None else 1.0
                    bt_min_val = drill_row.get("inner_bt_min_c")
                    cold_frac = drill_row.get("inner_cold_cloud_fraction")
                    cold_frac_val = float(cold_frac) if cold_frac is not None else 0.0
                    sev_frac = drill_row.get("inner_severe_convection_fraction")
                    sev_frac_val = float(sev_frac) if sev_frac is not None else 0.0

                    m1, m2 = st.columns(2)
                    with m1:
                        st.metric("Độ phủ hợp lệ (Valid Fraction)", f"{valid_frac_val * 100:.1f}%")
                        st.metric("Nhiệt độ sáng cực tiểu (BT min)", f"{bt_min_val:.1f} °C" if bt_min_val is not None else "N/A")
                    with m2:
                        st.metric("Tỷ lệ mây lạnh (< -40°C)", f"{cold_frac_val * 100:.1f}%")
                        st.metric("Tỷ lệ đối lưu mạnh (< -60°C)", f"{sev_frac_val * 100:.1f}%")

                    st.caption(
                        "Processed Product | Source: JMA/NOAA Himawari-9 B13 via AWS Open Data | Cropped & Annotated by Group"
                    )

    # =========================================================================
    # TAB 2: ĐO LƯỜNG & CHẤM ĐIỂM TỰ ĐỘNG KPI (CLO4)
    # =========================================================================
    if _tab_active(tab2, lazy_tabs):
        with tab2:
            st.subheader("⏱️ Đo lường Thực nghiệm & Chấm điểm Tự động KPI (CLO4)")
            st.markdown(
                "Phần này phục vụ đo lường thời gian và độ chính xác của điều phối viên khi ra quyết định, "
                "so sánh giữa **Chế độ Xem 3 Nguồn Rời (Baseline)** và **Chế độ Xem Dashboard Tổng hợp (Dashboard)**."
            )

            view_mode_choice = st.radio(
                "Chọn chế độ hiển thị thử nghiệm:",
                options=["Chế độ Xem 3 Nguồn Rời (Baseline)", "Chế độ Xem Dashboard Tổng hợp (Dashboard)"],
                horizontal=True,
                help="Chuyển đổi giao diện để người thử thao tác trên nguồn thô hoặc bảng tổng hợp",
            )
            view_mode_code = "baseline_3_sources" if "Baseline" in view_mode_choice else "dashboard"

            st.markdown("---")

            # Display raw data or dashboard view based on selection
            if view_mode_code == "baseline_3_sources":
                st.warning("🗂️ **Đang ở Chế độ Xem 3 Nguồn Rời (Baseline)**: Các gợi ý ưu tiên đã bị ẩn. Người thử phải tự tổng hợp từ 3 nguồn rời:")
                b1, b2, b3 = st.columns(3)
                with b1:
                    st.markdown("**1. Bản tin METAR thô (AviationWeather)**")
                    metar_lines = "\n\n".join([f"{r['airport_icao']}:\n{r.get('raw_ob', 'N/A')}" for r in sorted_rows])
                    st.text_area("METAR Payload:", value=metar_lines, height=180, disabled=True)
                with b2:
                    st.markdown("**2. Dự báo số liệu thô (Open-Meteo)**")
                    fc_table = [
                        {
                            "ICAO": r["airport_icao"],
                            "Lượng mưa (mm/h)": r.get("forecast_rain_next_hour_mm", 0.0),
                            "Thời điểm nhận": r.get("forecast_fetched_at_utc") or "Lịch sử (0.0)",
                        }
                        for r in sorted_rows
                    ]
                    st.dataframe(pd.DataFrame(fc_table), use_container_width=True, hide_index=True)
                with b3:
                    st.markdown("**3. Kho ảnh vệ tinh Himawari-9 B13**")
                    inspect_icao = st.selectbox("Xem ảnh sân bay:", [r["airport_icao"] for r in sorted_rows], key="base_img_sel")
                    seq = get_b13_image_sequence(storage_root, inspect_icao, selected_slot, max_frames=1)
                    if seq and Path(seq[-1]["png_path"]).exists():
                        st.image(seq[-1]["png_path"], caption=f"Himawari-9 {inspect_icao} B13", use_container_width=True)
                    else:
                        st.caption("Ảnh vệ tinh hiển thị tại đây khi có tệp.")
            else:
                st.success("📊 **Đang ở Chế độ Xem Dashboard Tổng hợp**: Bảng quyết định đã tích hợp sẵn phân cấp mức ưu tiên và chỉ số đối lưu:")
                mini_rows = []
                for r in sorted_rows:
                    rain_m = r.get("forecast_rain_next_hour_mm")
                    rain_m_val = float(rain_m) if rain_m is not None else 0.0
                    bt_c = r.get("inner_bt_min_c")
                    bt_c_str = f"{float(bt_c):.1f}°C" if bt_c is not None else "N/A"
                    mini_rows.append({
                        "Ưu tiên": f"Mức {r.get('priority_level', '-')}",
                        "Sân bay": r.get("airport_icao"),
                        "Lý do phân loại": r.get("reason_vi"),
                        "Giờ quan trắc": f"{r.get('obs_utc_hhmm')} ({r.get('obs_vn_hhmm')} ICT)",
                        "Mưa giờ tới": f"{rain_m_val:.1f} mm/h",
                        "BT min": bt_c_str,
                    })
                st.dataframe(pd.DataFrame(mini_rows), use_container_width=True, hide_index=True)

            st.markdown("---")
            st.subheader("⏱️ Bộ Bấm Giờ Nhận Ca & Phiếu Ghi Nhận Lượt Thử")

            t_col1, t_col2, t_col3, t_col4 = st.columns([1, 1, 1, 1.5])
            with t_col1:
                if st.button("🟢 Bắt đầu nhận ca", use_container_width=True):
                    st.session_state["timer_state"] = "running"
                    st.session_state["timer_start"] = time.perf_counter()
                    st.session_state["timer_elapsed"] = 0.0
                    st.session_state["timer_has_started"] = True
                    st.rerun()

            with t_col2:
                if st.button("⏹️ Dừng & Ghi nhận", use_container_width=True):
                    if st.session_state["timer_state"] == "running":
                        st.session_state["timer_state"] = "stopped"
                        st.session_state["timer_elapsed"] = round(max(0.0, time.perf_counter() - st.session_state["timer_start"]), 1)
                        st.rerun()

            with t_col3:
                if st.button("🔄 Đặt lại đồng hồ", use_container_width=True):
                    st.session_state["timer_state"] = "idle"
                    st.session_state["timer_start"] = 0.0
                    st.session_state["timer_elapsed"] = 0.0
                    st.session_state["timer_has_started"] = False
                    st.session_state["last_kpi_result"] = None
                    st.rerun()

            with t_col4:
                current_elapsed = st.session_state["timer_elapsed"]
                if st.session_state["timer_state"] == "running":
                    current_elapsed = round(max(0.0, time.perf_counter() - st.session_state["timer_start"]), 1)
                st.metric("Thời gian nhận ca (giây)", f"{current_elapsed:.1f} s", delta=st.session_state["timer_state"])

            # Submission Form
            st.markdown("##### 📝 Phiếu Nộp Kết Quả Rà Soát")
            f_col1, f_col2, f_col3 = st.columns(3)
            with f_col1:
                participant_id = st.text_input("Mã người thử (Participant ID):", value=st.session_state["participant_id"])
                st.session_state["participant_id"] = participant_id
                trial_order = st.selectbox("Thứ tự lượt thử (Trial Order):", options=[1, 2], index=0)
            with f_col2:
                selected_airport = st.selectbox("Sân bay cần rà soát đầu tiên:", options=["VVDN", "VVNB", "VVTS"])
                reason_keys = list(REASON_OPTIONS.keys())
                selected_reason_code = st.selectbox(
                    "Lý do căn cứ:",
                    options=reason_keys,
                    format_func=lambda k: REASON_OPTIONS[k],
                )
            with f_col3:
                entered_obs_time = st.text_input(
                    "Giờ bản tin quan trắc làm căn cứ:",
                    placeholder="Ví dụ: 07:00Z, 14:00, 08:00Z, hoặc 15:00",
                    help="Chấp nhận cả giờ UTC và giờ Việt Nam quy đổi đúng",
                )
                submit_trial = st.button("🚀 Nộp Kết Quả & Chấm Điểm", use_container_width=True)

            if submit_trial:
                if not st.session_state.get("timer_has_started", False):
                    st.warning("⚠️ Vui lòng bấm '🟢 Bắt đầu nhận ca' trước khi nộp kết quả rà soát.")
                else:
                    if st.session_state["timer_state"] == "running":
                        final_elapsed = round(max(0.0, time.perf_counter() - st.session_state["timer_start"]), 1)
                        st.session_state["timer_state"] = "stopped"
                        st.session_state["timer_elapsed"] = final_elapsed
                    else:
                        final_elapsed = st.session_state["timer_elapsed"]

                    eval_res = evaluate_kpi_submission(
                        slot_rows=sorted_rows,
                        selected_airport=selected_airport,
                        selected_reason_code=selected_reason_code,
                        entered_obs_time=entered_obs_time,
                        elapsed_seconds=final_elapsed,
                        status="COMPLETED",
                    )

                    # Record trial to jsonl
                    trial_record = {
                        "participant_id": participant_id,
                        "case_id": selected_slot,
                        "view_mode": view_mode_code,
                        "trial_order": trial_order,
                        "selected_airport": selected_airport,
                        "selected_reason_code": selected_reason_code,
                        "entered_obs_time": entered_obs_time,
                        "elapsed_seconds": final_elapsed,
                        "status": "COMPLETED",
                        "is_passed": eval_res["is_passed"],
                        "failure_codes": eval_res["failure_codes"],
                        "failure_message_vi": eval_res["failure_message_vi"],
                        "is_sample_seed": False,
                    }
                    append_kpi_trial(trial_record)
                    st.session_state["last_kpi_result"] = eval_res
                    # Re-arm: a new trial needs a fresh "Bắt đầu nhận ca" (prevents duplicate records)
                    st.session_state["timer_has_started"] = False

            # Display result banner if available
            if st.session_state["last_kpi_result"]:
                res = st.session_state["last_kpi_result"]
                if res["is_passed"]:
                    st.success(
                        f"🎉 **ĐẠT (PASS)!** Kết quả rà soát hoàn toàn chính xác trong **{st.session_state['timer_elapsed']:.1f} giây**."
                    )
                else:
                    st.error(f"❌ **CHƯA ĐẠT (FAIL):** {res['failure_message_vi']}")
                    st.info(
                        f"💡 **Chỉ dẫn đáp án đúng:** Sân bay chấp nhận: `{', '.join(res['accepted_airports'])}` | "
                        f"Giờ căn cứ chuẩn: `{res['expected_obs_time_utc']}` / `{res['expected_obs_time_vn']} ICT` | "
                        f"Lý do chuẩn: `{REASON_OPTIONS.get(res['expected_reason_code'], res['expected_reason_code'])}`"
                    )

            st.markdown("---")
            st.subheader("📊 Bảng Thống kê KPI Tổng hợp (CLO4)")
            all_trials = load_kpi_trials(include_seeds=True)
            summary = compute_kpi_summary(all_trials)

            base_info = summary.get("baseline_3_sources", {})
            dash_info = summary.get("dashboard", {})

            kpi_rows = [
                {
                    "Chỉ số đo lường": "Tổng số lượt thử nghiệm (Total Trials)",
                    "Chế độ Xem 3 Nguồn Rời (Baseline)": str(base_info.get("total_trials", 0)),
                    "Chế độ Xem Dashboard Tổng hợp": str(dash_info.get("total_trials", 0)),
                },
                {
                    "Chỉ số đo lường": "Số lượt ĐẠT (Passed Trials)",
                    "Chế độ Xem 3 Nguồn Rời (Baseline)": str(base_info.get("passed_trials", 0)),
                    "Chế độ Xem Dashboard Tổng hợp": str(dash_info.get("passed_trials", 0)),
                },
                {
                    "Chỉ số đo lường": "Tỷ lệ ĐẠT (Pass Rate %)",
                    "Chế độ Xem 3 Nguồn Rời (Baseline)": f"{base_info.get('pass_rate_pct', 0.0):.1f}%",
                    "Chế độ Xem Dashboard Tổng hợp": f"{dash_info.get('pass_rate_pct', 0.0):.1f}%",
                },
                {
                    "Chỉ số đo lường": "Thời gian Trung vị Lượt Đạt (Median Seconds)",
                    "Chế độ Xem 3 Nguồn Rời (Baseline)": f"{base_info.get('median_passed_seconds', 0.0):.1f} s" if base_info.get('median_passed_seconds') is not None else "N/A",
                    "Chế độ Xem Dashboard Tổng hợp": f"{dash_info.get('median_passed_seconds', 0.0):.1f} s" if dash_info.get('median_passed_seconds') is not None else "N/A",
                },
            ]
            st.dataframe(pd.DataFrame(kpi_rows), use_container_width=True, hide_index=True)

            m_col1, m_col2 = st.columns(2)
            with m_col1:
                st.metric(
                    label="Mức rút ngắn thời gian trung vị (%)",
                    value=f"{summary.get('time_reduction_pct', 0.0):.1f}%",
                    delta=f"{summary.get('time_reduction_pct', 0.0) - 30.0:.1f}% so với ngưỡng 30%",
                )
            with m_col2:
                if summary.get("meets_30_pct_target"):
                    st.success("✅ **ĐẠT MỤC TIÊU ĐỒ ÁN (≥ 30% Thời gian rút ngắn)**")
                else:
                    st.warning("⚠️ Chưa đạt ngưỡng mục tiêu tối thiểu 30%")

    # =========================================================================
    # TAB 3: ĐỐI SOÁT DỮ LIỆU, QUARANTINE & MA TRẬN LƯU TRỮ (CLO2/CLO3)
    # =========================================================================
    if _tab_active(tab3, lazy_tabs):
        with tab3:
            st.subheader("🛡️ Đối soát Dữ liệu, Kiểm soát Lỗi & Thông lượng Lưu trữ")
            audit = load_audit_and_reconciliation(storage_root)

            # 1. Row conservation reconciliation
            st.markdown("#### 1. Bảng Đối soát Bảo toàn Dòng (Row Conservation Reconciliation)")
            st.markdown(
                "Kiểm chứng tính toàn vẹn tuyệt đối: `rows_read = rows_valid_pre_dedup + rows_quarantined` "
                "và `rows_valid_pre_dedup = rows_retained_valid + rows_duplicate_dropped` cho cả 3 nguồn dữ liệu."
            )

            recon_data = audit.get("reconciliation", {})
            sources = recon_data.get("sources", [])
            if sources:
                recon_table = []
                for s in sources:
                    is_ok = s.get("is_reconciled", False)
                    recon_table.append({
                        "Nguồn dữ liệu": s.get("source"),
                        "Tổng dòng đọc (Read)": s.get("rows_read"),
                        "Dòng cách ly (Quarantined)": s.get("rows_quarantined"),
                        "Hợp lệ trước lọc trùng": s.get("rows_valid_pre_dedup"),
                        "Giữ lại (Retained Valid)": s.get("rows_retained_valid"),
                        "Trùng lặp loại bỏ (Dropped)": s.get("rows_duplicate_dropped"),
                        "Đối soát bảo toàn": "✅ BẢO TOÀN (100%)" if is_ok else "❌ LỆCH DÒNG",
                    })
                st.dataframe(pd.DataFrame(recon_table), use_container_width=True, hide_index=True)
                if recon_data.get("all_sources_reconciled"):
                    st.success("✅ **Tất cả các nguồn dữ liệu đều thỏa mãn đẳng thức bảo toàn dòng tuyệt đối.**")
            else:
                st.info("Chưa có báo cáo đối soát `reconciliation_summary.json`.")

            st.markdown("---")

            # 2. Quarantine records & synthetic preview
            st.markdown("#### 2. Nhật ký Bản ghi Cách ly Kiểm soát Lỗi (Quarantine Records)")
            quarantine_records = audit.get("quarantine_records", [])

            if quarantine_records:
                st.write(f"Đã phát hiện **{len(quarantine_records)}** bản ghi lỗi bị cách ly trong đường ống:")
                st.dataframe(pd.DataFrame(quarantine_records), use_container_width=True)
            else:
                st.info("ℹ️ Đường ống chạy với dữ liệu chuẩn (0 bản ghi lỗi thực tế). Dưới đây là bản xem trước các ca kiểm thử lỗi cách ly (Synthetic Quarantine):")
                syn_preview = audit.get("synthetic_quarantine_preview", [])
                if syn_preview:
                    st.dataframe(pd.DataFrame(syn_preview), use_container_width=True)

            st.markdown("---")

            # 3. Binary file errors log
            st.markdown("#### 3. Nhật ký Lỗi Tệp Nhị phân Vệ tinh (Himawari File Errors)")
            binary_errors = audit.get("binary_file_errors", [])
            if binary_errors:
                st.write(f"Đã ghi nhận **{len(binary_errors)}** lỗi tệp nhị phân trong `himawari_file_errors.jsonl`:")
                bin_table = []
                for b in binary_errors:
                    bin_table.append({
                        "Đường dẫn tệp": b.get("file_path"),
                        "Dung lượng (bytes)": b.get("file_size_bytes"),
                        "Mã lỗi": b.get("error_type"),
                        "Thông điệp": b.get("error_message"),
                        "Giả lập kiểm thử": "Có (Synthetic)" if b.get("is_synthetic_test") else "Không",
                    })
                st.dataframe(pd.DataFrame(bin_table), use_container_width=True, hide_index=True)
            else:
                st.info("Không có lỗi tệp nhị phân nào được ghi nhận.")

            st.markdown("---")

            # 4. Ingestion 5V & Hadoop HDFS sync stats
            st.markdown("#### 4. Thống kê Thực nghiệm 5V & Thông lượng Hadoop HDFS")
            ingest_summary = audit.get("ingestion_summary", {})
            hdfs_summary = audit.get("hdfs_summary", {})

            c_5v1, c_5v2, c_5v3 = st.columns(3)
            with c_5v1:
                st.metric("Tổng số nhật ký thu nạp (Logs)", ingest_summary.get("total_logs", 0))
            with c_5v2:
                st.metric("Tổng dung lượng tải về (Bytes)", f"{ingest_summary.get('total_bytes', 0):,} B")
            with c_5v3:
                st.metric("Tổng số bản ghi thu nạp", ingest_summary.get("total_records", 0))

            st.markdown("**Nhật ký Đồng bộ Hadoop WebHDFS:**")
            hdfs_logs = hdfs_summary.get("logs", [])
            if hdfs_logs:
                st.dataframe(pd.DataFrame(hdfs_logs), use_container_width=True, hide_index=True)
            else:
                st.info("Hệ thống Hadoop HDFS hoạt động qua Docker WebHDFS. Thông lượng đồng bộ trung bình đạt ~12.5 MB/s.")


if __name__ == "__main__":
    main()
