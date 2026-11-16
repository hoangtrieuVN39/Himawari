"""Tab "Sức khỏe hệ thống": operational diagnostics rendered from the Gold ``health/`` artifacts.

Read-only on render: summary.json and Parquet tables written by ``src.diagnostics.run_health``. Heavy
computation only happens when the user presses "Tính lại chỉ số sức khỏe".
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

from src.dashboard import components as c

HEALTH_SUBDIR = Path("lake") / "gold" / "health"
SUMMARY_FILENAME = "summary.json"
GAP_ROWS_SHOWN = 300

SOURCE_LABELS = {"metar": "METAR (AWC)", "open_meteo": "Open-Meteo", "himawari_hsd": "Himawari-9 B13", "himawari": "Himawari-9 B13"}
STATUS_LABELS = {"ok": "Đạt", "breach": "Vi phạm SLA", "no_data": "Chưa có dữ liệu", "stale": "Quá hạn", "not_available": "Không khả dụng"}
CAUSE_LABELS = {
    "SOURCE_MISSING": "Nguồn thiếu (S3/API không có)",
    "RECEIVER_FAULT": "Lỗi máy thu (nguồn có, ta không xử lý)",
    "UNKNOWN": "Chưa xác định (chưa kiểm tra nguồn)",
    "FETCHED_NO_NEW_OBSERVATION": "Đã lấy nhưng chưa có quan trắc mới",
}
METRIC_LABELS = {
    "hourly_availability": "Giờ có dữ liệu (độ sẵn sàng theo giờ)",
    "fetch_age_minutes": "Tuổi lần lấy gần nhất (phút)",
    "himawari_scan_coverage": "Độ phủ lượt quét Himawari (đã xử lý / dự kiến)",
    "himawari_download_coverage": "Độ phủ tải về Himawari",
    "himawari_gap_free_slots": "Mốc Himawari không khuyết",
    "ingest_cycle_completeness": "Chu kỳ thu nạp đủ (so với 10′/60′)",
    "metar_cycles_with_new_observation": "Chu kỳ METAR có quan trắc mới",
    "decision_full_evidence_share": "Dòng đủ bằng chứng (METAR + dự báo + B13)",
    "decision_fresh_metar_share": "Dòng có METAR còn mới (≤ 60′)",
    "decision_full_evidence_fresh_share": "Dòng đủ bằng chứng và METAR còn mới",
    "pipeline_latency_p50_s": "Độ trễ pipeline P50 (giây)",
    "pipeline_latency_p95_s": "Độ trễ pipeline P95 (giây)",
    "runner_heartbeat_age_minutes": "Tuổi heartbeat runner (phút)",
}
FEATURE_LABELS = {
    "inner_bt_min_c": "BT tối thiểu hộp trong (°C)",
    "inner_cold_cloud_fraction": "Tỷ lệ mây lạnh hộp trong",
    "visibility_m": "Tầm nhìn METAR (m)",
    "forecast_rain_mm": "Mưa dự báo giờ tới (mm)",
}
VOLUME_KIND_LABELS = {"downloaded_window": "Tải về trong khoảng đo", "retained": "Lưu giữ trên đĩa", "retained_total": "Tổng lưu giữ"}


# ---------------------------------------------------------------------------
# Pure formatting helpers (unit-testable)
# ---------------------------------------------------------------------------

def health_dir(storage_root: str) -> Path:
    return Path(storage_root) / HEALTH_SUBDIR


def load_summary(storage_root: str) -> Optional[dict[str, Any]]:
    path = health_dir(storage_root) / SUMMARY_FILENAME
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def load_table(storage_root: str, name: str) -> Optional[pd.DataFrame]:
    path = health_dir(storage_root) / f"{name}.parquet"
    if not path.is_file():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:  # noqa: BLE001 - a half-written/corrupt file must not break the page
        return None


def _num(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def fmt_value(m: dict[str, Any]) -> str:
    """Human value of a metric row: percentage for ratios, plain number + unit otherwise."""
    if m.get("status") == "not_available":
        return "không khả dụng"
    value = _num(m.get("value"))
    if value is None:
        return "—"
    unit = m.get("unit")
    if unit == "ratio":
        return f"{value * 100:.1f}%"
    if unit == "seconds":
        return f"{value:.1f} s"
    if unit == "minutes":
        return f"{value:.1f} phút"
    return f"{value:.3g}"


def fmt_hours(hours: Any, limit: int = 4) -> str:
    items = [str(h) for h in (hours if hours is not None else [])]
    if not items:
        return "—"
    short = [h[5:13].replace("T", " ") + "h" for h in items[:limit]]
    return ", ".join(short) + (f" … (+{len(items) - limit})" if len(items) > limit else "")


def fmt_minutes(value: Any) -> str:
    v = _num(value)
    return "—" if v is None else f"{v:.1f}"


def fmt_pct(value: Any) -> str:
    v = _num(value)
    return "—" if v is None else f"{v * 100:.1f}%"


def _time(value: Any) -> str:
    return "—" if not value else str(value).replace("T", " ").replace("Z", "")


def _window(m: dict[str, Any]) -> str:
    return f"{_time(m.get('window_start_utc'))} → {_time(m.get('window_end_utc'))}"


def metric_rows(metrics: list[dict[str, Any]], prefixes: tuple[str, ...]) -> list[list[Any]]:
    """Rows [Chỉ số, Phạm vi, Giá trị, Tử số / mẫu số, Khoảng đo (UTC), Giờ thiếu] for matching metrics."""
    rows = []
    for m in metrics:
        if not str(m.get("name", "")).startswith(prefixes):
            continue
        num, den = _num(m.get("numerator")), _num(m.get("denominator"))
        rows.append([
            METRIC_LABELS.get(m["name"], m["name"]),
            SOURCE_LABELS.get(m.get("scope"), m.get("scope")),
            fmt_value(m),
            f"{num:g} / {den:g}" if num is not None and den is not None else "—",
            _window(m),
            f"{int(m.get('missing_hours_count') or 0)}: {fmt_hours(m.get('missing_hours'))}",
        ])
    return rows


def find_metric(metrics: list[dict[str, Any]], name: str, scope: str) -> Optional[dict[str, Any]]:
    return next((m for m in metrics if m.get("name") == name and m.get("scope") == scope), None)


def freshness_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    out = []
    for r in rows:
        out.append([
            SOURCE_LABELS.get(r.get("source"), r.get("source")),
            r.get("scope"),
            _time(r.get("last_fetched_at_utc")),
            fmt_minutes(r.get("age_minutes")),
            fmt_minutes(r.get("sla_minutes")),
            _time(r.get("last_observation_at_utc")),
            fmt_minutes(r.get("observation_age_minutes")),
            fmt_minutes(r.get("observation_sla_minutes")),
            STATUS_LABELS.get(r.get("status"), r.get("status")),
        ])
    return out


def coverage_day_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [[
        r.get("day_utc") + (" (một phần)" if r.get("is_partial_day") else ""),
        r.get("expected_scans"), r.get("downloaded_scans"), r.get("processed_scans"),
        fmt_pct(r.get("coverage_ratio")), r.get("missing_scans"), r.get("backfilled_scans"),
        f"{int(r.get('missing_hours_count') or 0)}: {fmt_hours(r.get('missing_hours'), 3)}",
    ] for r in rows]


def gap_summary_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [[
        SOURCE_LABELS.get(r.get("source"), r.get("source")),
        CAUSE_LABELS.get(r.get("cause"), r.get("cause")),
        r.get("count"), r.get("total"), fmt_pct(r.get("share")),
    ] for r in rows]


def drift_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    out = []
    for r in rows:
        psi = _num(r.get("psi"))
        if r.get("status") != "ok":
            verdict = "Không đủ mẫu"
        else:
            verdict = "TRÔI (PSI > ngưỡng)" if r.get("drift_flag") else "Ổn định"
        out.append([
            FEATURE_LABELS.get(r.get("feature"), r.get("feature")),
            "—" if psi is None else f"{psi:.3f}",
            f"{r.get('n_current')} / {r.get('n_reference')}",
            f"{_time(r.get('current_start_utc'))} → {_time(r.get('current_end_utc'))}",
            f"{int(r.get('missing_hours_count') or 0)}: {fmt_hours(r.get('missing_hours'), 3)}",
            verdict,
        ])
    return out


def volume_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [[
        VOLUME_KIND_LABELS.get(r.get("kind"), r.get("kind")), r.get("name"),
        c.format_bytes(r.get("bytes")), f"{int(r.get('files') or 0):,}",
    ] for r in rows]


def stage_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [[r.get("stage"), r.get("n"), f"{_num(r.get('median_s')) or 0:.2f}", f"{_num(r.get('p95_s')) or 0:.2f}"] for r in rows]


def anomaly_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [[
        SOURCE_LABELS.get(r.get("source"), r.get("source")), r.get("metric"), _time(r.get("cycle_at_utc")),
        f"{_num(r.get('value')) or 0:,.1f}", f"{_num(r.get('median')) or 0:,.1f}", f"{_num(r.get('robust_z')) or 0:.1f}",
    ] for r in rows]


# ---------------------------------------------------------------------------
# Streamlit rendering
# ---------------------------------------------------------------------------

def _tone_for_status(status: Optional[str]) -> str:
    return {"ok": "ok", "breach": "danger", "stale": "danger", "no_data": "warn", "not_available": "warn"}.get(status or "", "neutral")


def _header_tiles(summary: dict[str, Any]) -> str:
    metrics = summary.get("metrics") or []
    hb = summary.get("heartbeat") or {}
    tiles = []

    cov = find_metric(metrics, "himawari_scan_coverage", "ALL")
    if cov:
        tiles.append(c.stat_tile("Độ phủ Himawari", fmt_value(cov),
                                 f"{cov['numerator']:g}/{cov['denominator']:g} lượt đủ phân đoạn",
                                 "warn" if (cov.get("value") or 0) < 0.9 else ""))
    full = find_metric(metrics, "decision_full_evidence_share", "window") or {}
    tiles.append(c.stat_tile("Dòng đủ bằng chứng (24h)", fmt_value(full) if full else "—",
                             f"mẫu số {full.get('denominator', 0):g} dòng" if full else None))
    p50 = find_metric(metrics, "pipeline_latency_p50_s", "ALL")
    p95 = find_metric(metrics, "pipeline_latency_p95_s", "ALL")
    if p50 and p95:
        tiles.append(c.stat_tile("Độ trễ P50 / P95", f"{fmt_value(p50)} / {fmt_value(p95)}",
                                 f"n = {p50.get('numerator', 0):g} lần chạy"))
    if hb.get("present"):
        tiles.append(c.stat_tile("Heartbeat runner", f"{fmt_minutes(hb.get('age_minutes'))} phút",
                                 f"trạng thái {hb.get('status') or '—'}", "warn" if hb.get("is_stale") else ""))
    else:
        tiles.append(c.stat_tile("Heartbeat runner", "chưa có", "runner Spark chưa ghi heartbeat", "warn"))
    return c.tile_grid(tiles, cols=min(max(len(tiles), 2), 4))


def _recompute(storage_root: str, check_source: bool) -> bool:
    from src.diagnostics.run_health import run_health

    with st.spinner("Đang tính chỉ số sức khỏe…"):
        try:
            summary = run_health(storage_root, 24, check_source)
        except Exception as exc:  # noqa: BLE001 - surface to the operator instead of a stack trace page
            st.error(f"Không tính được chỉ số sức khỏe: {type(exc).__name__}: {exc}")
            return False
    if summary.get("errors"):
        st.warning("Một số mục không tính được: " + ", ".join(e["section"] for e in summary["errors"]))
    else:
        st.success("Đã cập nhật chỉ số sức khỏe.")
    return True


def render_health_tab(storage_root: str) -> None:
    """Renders the health tab from summary.json / Parquet (never computes metrics on render)."""
    st.html(c.section_header("Sức khỏe hệ thống", "Chẩn đoán vận hành: độ mới, độ phủ, nguyên nhân khoảng thiếu, bất thường, trôi dữ liệu"))

    col_btn, col_chk = st.columns([1, 2])
    with col_chk:
        check_source = st.checkbox("Kiểm tra nguồn S3 (chậm hơn, phân loại khoảng thiếu Himawari)", key="health_check_source")
    with col_btn:
        pressed = st.button("Tính lại chỉ số sức khỏe", key="health_recompute")
    if pressed:
        _recompute(storage_root, bool(check_source))

    summary = load_summary(storage_root)
    if not summary:
        st.info("Chưa có chỉ số sức khỏe. Bấm “Tính lại chỉ số sức khỏe” hoặc chạy `python -m src.diagnostics.run_health --storage-root data`.")
        return

    window = summary.get("window") or {}
    st.html(
        f'<div class="muted">Tính lúc {c._e(_time(summary.get("generated_at_utc")))} UTC · khoảng đo '
        f'{c._e(_time(window.get("start_utc")))} → {c._e(_time(window.get("end_utc")))} '
        f'({c._e(window.get("hours"))} giờ) · kiểm tra nguồn S3: {"có" if summary.get("source_check") else "không"}</div>'
    )
    for err in summary.get("errors") or []:
        st.warning(f"Mục “{err.get('section')}” lỗi khi tính: {err.get('error')}")

    metrics: list[dict[str, Any]] = summary.get("metrics") or []
    st.html(_header_tiles(summary))

    # 1. Freshness SLA
    st.html(c.section_header("1 · Độ mới nguồn (SLA)", "SLA lấy = 2× chu kỳ; METAR còn cần tuổi quan trắc ≤ 60′"))
    fresh = summary.get("freshness") or []
    if fresh:
        st.html(c.data_table(
            ["Nguồn", "Phạm vi", "Lấy lần cuối (UTC)", "Tuổi (phút)", "SLA (phút)", "Quan trắc cuối (UTC)", "Tuổi QT (phút)", "SLA QT", "Trạng thái"],
            freshness_rows(fresh), mono_cols=(2, 3, 4, 5, 6, 7),
        ))
    else:
        st.info("Chưa có dữ liệu độ mới.")
    avail = metric_rows(metrics, ("hourly_availability",))
    if avail:
        st.html(c.data_table(["Chỉ số", "Nguồn", "Giá trị", "Tử số / mẫu số", "Khoảng đo (UTC)", "Giờ thiếu"], avail, mono_cols=(2, 3, 4)))

    # 2. Himawari coverage
    st.html(c.section_header("2 · Độ phủ Himawari theo ngày", "144 lượt/ngày/phân đoạn; “đủ phân đoạn” = cả S0410 và S0510 đã xử lý"))
    cov_rows = summary.get("coverage_all_segments") or []
    if cov_rows:
        st.html(c.data_table(
            ["Ngày (UTC)", "Dự kiến", "Đã tải", "Đã xử lý đủ phân đoạn", "Độ phủ", "Thiếu", "Tải bù", "Giờ mất hoàn toàn"],
            coverage_day_rows(cov_rows), mono_cols=(1, 2, 3, 4, 5, 6),
        ))
    cov_df = load_table(storage_root, "himawari_coverage")
    if cov_df is not None and not cov_df.empty:
        with st.expander("Độ phủ theo phân đoạn và theo sân bay"):
            detail = cov_df[cov_df["scope_type"] != "all_segments"][
                ["day_utc", "scope_type", "scope", "expected_scans", "downloaded_scans", "processed_scans", "coverage_ratio", "missing_scans"]
            ]
            st.dataframe(detail, width="stretch", hide_index=True)
    cov_metrics = metric_rows(metrics, ("himawari_",))
    if cov_metrics:
        with st.expander("Chỉ số Himawari trong khoảng đo (kèm mẫu số, giờ thiếu)"):
            st.html(c.data_table(["Chỉ số", "Phạm vi", "Giá trị", "Tử số / mẫu số", "Khoảng đo (UTC)", "Giờ thiếu"], cov_metrics, mono_cols=(2, 3, 4)))

    # 3. Gap attribution
    st.html(c.section_header("3 · Nguyên nhân khoảng thiếu", "Tệp có trên S3 mà ta không xử lý = lỗi máy thu; không có trên S3 = nguồn thiếu"))
    gaps = summary.get("gap_summary") or []
    if gaps:
        if not summary.get("source_check"):
            st.caption("Chưa kiểm tra nguồn S3 nên khoảng thiếu Himawari chưa có bằng chứng ở nhóm “Chưa xác định”.")
        st.html(c.data_table(["Nguồn", "Nguyên nhân", "Số lượng", "Tổng khoảng thiếu", "Tỷ lệ"], gap_summary_rows(gaps), mono_cols=(2, 3, 4)))
    else:
        st.info("Không có khoảng thiếu nào trong khoảng đo.")
    cycle = metric_rows(metrics, ("ingest_cycle", "metar_cycles"))
    if cycle:
        st.html(c.data_table(["Chỉ số", "Nguồn", "Giá trị", "Tử số / mẫu số", "Khoảng đo (UTC)", "Giờ thiếu"], cycle, mono_cols=(2, 3, 4)))
    gap_df = load_table(storage_root, "gap_attribution")
    if gap_df is not None and not gap_df.empty:
        with st.expander(f"Chi tiết {len(gap_df):,} khoảng thiếu (hiển thị {min(len(gap_df), GAP_ROWS_SHOWN)} gần nhất)"):
            st.dataframe(
                gap_df.sort_values("slot_utc", ascending=False).head(GAP_ROWS_SHOWN)[
                    ["source", "scope", "slot_utc", "cause", "local_evidence", "check_method"]
                ],
                width="stretch", hide_index=True,
            )

    # 4. Decision KPIs + pipeline
    st.html(c.section_header("4 · KPI bảng quyết định & độ trễ pipeline", "Độ trễ = lúc chạy xong − lúc nhận đủ đầu vào, theo từng lần chạy Spark"))
    kpi = metric_rows(metrics, ("decision_", "pipeline_latency", "runner_heartbeat"))
    if kpi:
        st.html(c.data_table(["Chỉ số", "Phạm vi", "Giá trị", "Tử số / mẫu số", "Khoảng đo (UTC)", "Giờ thiếu"], kpi, mono_cols=(2, 3, 4)))
    stages = summary.get("pipeline_stage_timings") or []
    if stages:
        st.html(c.data_table(["Giai đoạn", "Số lần chạy", "Trung vị (s)", "P95 (s)"], stage_rows(stages), mono_cols=(1, 2, 3)))
    else:
        st.caption("Chưa có `logs/pipeline_runs.jsonl` trong khoảng đo nên chưa có thời gian từng giai đoạn.")

    # 5. Anomalies
    counts = summary.get("anomaly_counts") or {}
    flagged = summary.get("anomalies_flagged") or []
    st.html(c.section_header(
        "5 · Bất thường (robust z-score, |z| > 3.5)", "Trung vị/MAD trên byte, số bản ghi và khoảng cách giữa các chu kỳ",
        c.pill(f"{counts.get('flagged', 0)} cờ / {counts.get('rows', 0)} điểm", "warn" if counts.get("flagged") else "ok"),
    ))
    if flagged:
        st.html(c.data_table(["Nguồn", "Đại lượng", "Chu kỳ (UTC)", "Giá trị", "Trung vị", "z"], anomaly_rows(flagged), mono_cols=(2, 3, 4, 5)))
    else:
        st.info("Không có bất thường nào bị gắn cờ.")

    # 6. Drift
    st.html(c.section_header("6 · Trôi dữ liệu (PSI, 24h gần nhất so với 72h trước đó)", "PSI > 0,2 bị gắn cờ trôi"))
    drift = summary.get("drift") or []
    if drift:
        st.html(c.data_table(["Đặc trưng", "PSI", "Mẫu (hiện tại / tham chiếu)", "Cửa sổ hiện tại (UTC)", "Giờ thiếu", "Kết luận"], drift_rows(drift), mono_cols=(1, 2, 3)))

    # 7. Profile
    st.html(c.section_header("7 · Hồ sơ dữ liệu theo nguồn (Bronze)", "Số dòng, tỷ lệ thiếu, tỷ lệ trùng chính xác, min / p50 / max"))
    prof = load_table(storage_root, "profile")
    if prof is not None and not prof.empty:
        totals = prof[prof["field"] == "*"]
        st.html(c.data_table(
            ["Nguồn", "Số dòng", "Dòng trùng", "Tỷ lệ trùng"],
            [[SOURCE_LABELS.get(r.source, r.source), f"{int(r.rows):,}", f"{int(r.duplicate_rows or 0):,}", fmt_pct(r.duplicate_rate)] for r in totals.itertuples()],
            mono_cols=(1, 2, 3),
        ))
        with st.expander("Từng trường (tỷ lệ thiếu, min / p50 / max)"):
            st.dataframe(
                prof[prof["field"] != "*"][["source", "field", "rows", "missing_count", "missing_rate", "min", "p50", "max"]],
                width="stretch", hide_index=True,
            )
    else:
        st.info("Chưa có hồ sơ dữ liệu.")

    # 8. Cost / volume
    st.html(c.section_header("8 · Chi phí & dung lượng", "Byte tải về trong khoảng đo và dung lượng đang giữ theo khu vực dưới data/"))
    volume = summary.get("volume") or []
    if volume:
        st.html(c.data_table(["Loại", "Tên", "Dung lượng", "Số tệp"], volume_rows(volume), mono_cols=(2, 3)))
