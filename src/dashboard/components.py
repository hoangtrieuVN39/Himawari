"""Pure HTML builders for the dashboard's display-only blocks (rendered with st.html).

Every function returns an HTML string and never calls Streamlit, so the markup is unit-testable.
All dynamic text goes through `_e()` (html.escape): METAR payloads and quarantine records are external data.
Interactive controls stay native Streamlit widgets; this module only draws what the user reads.
"""

from __future__ import annotations

from functools import lru_cache
import html
from pathlib import Path
from typing import Any, Iterable

from src.dashboard.data_loader import normalize_timestamp_pair
from src.spark.priority_rules import FORECAST_RAIN_ALERT_MM, LOW_VISIBILITY_M, METAR_STALE_MINUTES

THEME_CSS_PATH = Path(__file__).with_name("theme.css")

AIRPORT_SHORT_NAMES: dict[str, str] = {
    "VVDN": "Đà Nẵng",
    "VVNB": "Nội Bài",
    "VVTS": "Tân Sơn Nhất",
}

LEVEL_LABELS: dict[int, str] = {
    1: "Nguy hiểm tại trạm",
    2: "Cần xác minh dữ liệu",
    3: "Cảnh báo mưa dự báo",
    4: "Bình thường",
}
LEVEL_SHORT: dict[int, str] = {1: "Nguy hiểm", 2: "Xác minh", 3: "Mưa dự báo", 4: "Bình thường"}
# Shape glyphs so levels stay distinguishable without colour (colour-blind users, B/W print).
LEVEL_ICONS: dict[int, str] = {1: "▲", 2: "◆", 3: "●", 4: "○"}
LEVEL_COLORS: dict[int, dict[str, str]] = {
    1: {"fg": "#B42318", "bg": "#FEF3F2", "border": "#FECDCA"},
    2: {"fg": "#B54708", "bg": "#FFFAEB", "border": "#FEDF89"},
    3: {"fg": "#175CD3", "bg": "#EFF8FF", "border": "#B2DDFF"},
    4: {"fg": "#475467", "bg": "#F9FAFB", "border": "#D0D5DD"},
}


def _e(value: Any, default: str = "—") -> str:
    if value is None or value == "":
        return default
    return html.escape(str(value))


def _level(value: Any) -> int:
    try:
        lv = int(value)
    except (TypeError, ValueError):
        return 4
    return lv if lv in LEVEL_LABELS else 4


@lru_cache(maxsize=1)
def _theme_css_cached(mtime_ns: int) -> str:
    return THEME_CSS_PATH.read_text(encoding="utf-8")


def theme_style_tag() -> str:
    """<style> block with the dashboard theme (re-read when theme.css changes)."""
    return f"<style>{_theme_css_cached(THEME_CSS_PATH.stat().st_mtime_ns)}</style>"


def inject_theme() -> None:
    """Injects theme.css into the page (no visible element)."""
    import streamlit as st

    st.html(theme_style_tag())


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

def level_badge(level: Any, compact: bool = False) -> str:
    lv = _level(level)
    icon = LEVEL_ICONS[lv]
    if compact:
        return f'<span class="lv lv{lv} lv-compact" title="Mức {lv} · {LEVEL_LABELS[lv]}"><span class="lv-ic">{icon}</span>Mức {lv}</span>'
    return (
        f'<span class="lv lv{lv}"><span class="lv-ic">{icon}</span>'
        f'<span class="lv-num">Mức {lv}</span><span class="lv-txt">{LEVEL_LABELS[lv]}</span></span>'
    )


def level_legend() -> str:
    items = "".join(
        f'<span class="legend-item lv{lv}"><span class="lv-ic">{LEVEL_ICONS[lv]}</span>{lv} {LEVEL_SHORT[lv]}</span>'
        for lv in LEVEL_LABELS
    )
    return f'<div class="legend">{items}</div>'


def pill(text: str, tone: str = "neutral") -> str:
    return f'<span class="pill pill-{_e(tone)}">{_e(text)}</span>'


def time_pair(utc: Any, ict: Any) -> str:
    if not utc or utc == "N/A":
        return '<span class="muted">—</span>'
    return f'<span class="mono strong">{_e(utc)}</span><span class="sub mono">{_e(ict)} ICT</span>'


def section_header(title: str, subtitle: str | None = None, right_html: str = "") -> str:
    sub = f'<div class="sec-sub">{_e(subtitle)}</div>' if subtitle else ""
    right = f'<div class="sec-right">{right_html}</div>' if right_html else ""
    return f'<div class="sec-head"><div><div class="sec-title">{_e(title)}</div>{sub}</div>{right}</div>'


def stat_tile(label: str, value: Any, sub: str | None = None, tone: str = "") -> str:
    sub_html = f'<div class="tile-sub">{_e(sub)}</div>' if sub else ""
    return (
        f'<div class="tile {(" tile-" + tone) if tone else ""}"><div class="tile-label">{_e(label)}</div>'
        f'<div class="tile-value mono">{_e(value)}</div>{sub_html}</div>'
    )


def tile_grid(tiles: Iterable[str], cols: int = 2) -> str:
    return f'<div class="tile-grid cols{cols}">{"".join(tiles)}</div>'


def notice(title: str, body: str = "", tone: str = "info") -> str:
    """Inline notice block (for states rendered inside HTML cards)."""
    body_html = f'<div class="notice-body">{_e(body)}</div>' if body else ""
    return f'<div class="notice notice-{_e(tone)}"><div class="notice-title">{_e(title)}</div>{body_html}</div>'


# ---------------------------------------------------------------------------
# Header & sidebar
# ---------------------------------------------------------------------------

def sidebar_brand() -> str:
    return (
        '<div class="brand"><div class="brand-logo">✈</div><div>'
        '<div class="brand-title">Điều phối Thời tiết</div>'
        '<div class="brand-sub">Rà soát ưu tiên VVNB · VVDN · VVTS</div></div></div>'
    )


def sidebar_section(label: str) -> str:
    return f'<div class="side-sec">{_e(label)}</div>'


def sidebar_footer() -> str:
    return '<div class="side-foot">Nguồn: AviationWeather (METAR) · Open-Meteo · JMA Himawari-9 B13</div>'


def page_header(is_live: bool, slot_utc: str, slot_ict: str, slot_date: str, context: str | None = None) -> str:
    mode = '<span class="mode mode-live"><span class="dot"></span>LIVE</span>' if is_live else '<span class="mode mode-replay">PHÁT LẠI</span>'
    ctx = f'<span class="hdr-sep">·</span><span class="hdr-ctx">{_e(context)}</span>' if context else ""
    return (
        '<div class="page-head"><div class="page-title">Điều phối Thời tiết Sân bay</div>'
        f'<div class="page-meta">{mode}<span class="mono strong">{_e(slot_utc)}</span>'
        f'<span class="mono muted">{_e(slot_ict)} ICT</span><span class="hdr-sep">·</span>'
        f'<span>{_e(slot_date)}</span>{ctx}</div></div>'
    )


# ---------------------------------------------------------------------------
# Tab 1 — decision
# ---------------------------------------------------------------------------

def _fmt_age(age: Any) -> str:
    if age is None:
        return "N/A"
    return f"{int(round(float(age)))} phút"


def decision_banner(rows: list[dict[str, Any]]) -> str:
    """Top banner: one first-review airport, or the co-priority group of equivalent answers."""
    if not rows:
        return ""
    first = rows[0]
    accepted = list(first.get("accepted_first_review_airports") or [])
    if not accepted:
        top = min(_level(r.get("priority_level")) for r in rows)
        accepted = sorted(r["airport_icao"] for r in rows if _level(r.get("priority_level")) == top)
    by_icao = {r.get("airport_icao"): r for r in rows}
    lead = by_icao.get(accepted[0], first)
    lv = _level(lead.get("priority_level"))

    if len(accepted) > 1:
        chips = '<span class="eq">=</span>'.join(
            f'<span class="ban-chip"><span class="ban-code">{_e(c)}</span><span class="ban-name">{_e(AIRPORT_SHORT_NAMES.get(c, c))}</span></span>'
            for c in accepted
        )
        return (
            f'<div class="banner banner-lv{lv} banner-tie">'
            '<div class="ban-kicker">ĐỒNG MỨC ƯU TIÊN ĐẦU — CHỌN BẤT KỲ SÂN BAY TRONG NHÓM</div>'
            f'<div class="ban-chips">{chips}</div>'
            f'<div class="ban-level">{level_badge(lv)}</div>'
            f'<div class="ban-reason">{len(accepted)} sân bay cùng Mức {lv}. Chọn sân bay nào trong nhóm cũng đúng.</div>'
            "</div>"
        )

    present = sum(bool(lead.get(k)) for k in ("metar_is_present", "forecast_is_present", "b13_is_present"))
    if lead.get("forecast_is_present") is None and lead.get("forecast_rain_next_hour_mm") is not None:
        present += 1
    age = lead.get("metar_age_minutes")
    age_tone = "warn" if age is not None and float(age) > METAR_STALE_MINUTES else ""
    facts = tile_grid([
        stat_tile("Giờ bản tin", lead.get("obs_utc_hhmm") if lead.get("metar_is_present", True) else None,
                  f"{lead.get('obs_vn_hhmm')} ICT" if lead.get("metar_is_present", True) and lead.get("obs_vn_hhmm") else None),
        stat_tile("Tuổi METAR", _fmt_age(age), f"Ngưỡng {int(METAR_STALE_MINUTES)} phút", age_tone),
        stat_tile("Nguồn dữ liệu", f"{min(present, 3)}/3", "METAR · Dự báo · B13"),
    ], cols=3)
    return (
        f'<div class="banner banner-lv{lv}"><div class="ban-main">'
        '<div class="ban-kicker">SÂN BAY CẦN RÀ SOÁT ĐẦU TIÊN</div>'
        f'<div class="ban-airport"><span class="ban-code big">{_e(lead.get("airport_icao"))}</span>'
        f'<span class="ban-name">{_e(AIRPORT_SHORT_NAMES.get(lead.get("airport_icao"), ""))}</span></div>'
        f'<div class="ban-level">{level_badge(lv)}</div>'
        f'<div class="ban-reason">{_e(lead.get("reason_vi"))}</div></div>'
        f'<div class="ban-facts">{facts}</div></div>'
    )


def source_status(row: dict[str, Any]) -> str:
    missing = []
    if not row.get("metar_is_present"):
        missing.append("METAR")
    if not row.get("b13_is_present"):
        missing.append("B13")
    if missing:
        return f'<span class="pill pill-warn">Khuyết nguồn</span><span class="sub">thiếu {" · ".join(missing)}</span>'
    return '<span class="pill pill-ok">Đủ 3 nguồn</span>'


def _vis_cell(row: dict[str, Any]) -> str:
    vis = row.get("visibility_m")
    if vis is None:
        return '<span class="muted">N/A</span>'
    if row.get("visibility_capped_9999"):
        return '<span class="mono">9999m</span><span class="sub">(≥9999)</span>'
    try:
        low = float(vis) < LOW_VISIBILITY_M
    except (TypeError, ValueError):
        low = False
    cls = " danger" if row.get("is_low_visibility") or low else ""
    return f'<span class="mono{cls}">{_e(vis)}m</span>'


def _age_cell(row: dict[str, Any]) -> str:
    age = row.get("metar_age_minutes")
    if age is None:
        return '<span class="muted">N/A</span>'
    cls = " warn" if float(age) > METAR_STALE_MINUTES else ""
    return f'<span class="mono{cls}">{_fmt_age(age)}</span>'


def _rain_cell(row: dict[str, Any]) -> str:
    rain = row.get("forecast_rain_next_hour_mm")
    val = float(rain) if rain is not None else 0.0
    cls = " info" if val >= FORECAST_RAIN_ALERT_MM else ""
    return f'<span class="mono{cls}">{val:.1f} mm/h</span>'


def _b13_cell(row: dict[str, Any]) -> str:
    if not row.get("b13_is_present"):
        return '<span class="muted">Không có ảnh</span>'
    bt = row.get("inner_bt_min_c")
    bt_txt = f"{float(bt):.1f}°C" if bt is not None else "N/A"
    return f'{_e(row.get("b13_summary_vi"), "Bình thường")} <span class="sub mono">(BT min: {bt_txt})</span>'


def airport_table(rows: list[dict[str, Any]], selected_icao: str | None = None) -> str:
    head = "".join(f"<th>{h}</th>" for h in (
        "Mức ưu tiên", "Sân bay", "Lý do phân loại", "Giờ quan trắc", "Tuổi METAR",
        "Tầm nhìn", "Mưa giờ tới", "Mây B13 (vệ tinh)", "Nguồn",
    ))
    body = []
    for r in rows:
        icao = r.get("airport_icao", "")
        lv = _level(r.get("priority_level"))
        sel = " sel" if icao == selected_icao else ""
        obs = time_pair(r.get("obs_utc_hhmm"), r.get("obs_vn_hhmm")) if r.get("metar_is_present", True) else '<span class="muted">—</span>'
        body.append(
            f'<tr class="row-lv{lv}{sel}"><td class="nowrap">{level_badge(lv, compact=True)}<span class="sub">{LEVEL_LABELS[lv]}</span></td>'
            f'<td><span class="mono strong">{_e(icao)}</span><span class="sub">{_e(AIRPORT_SHORT_NAMES.get(icao, ""))}</span></td>'
            f'<td class="reason">{_e(r.get("reason_vi"))}</td><td class="nowrap">{obs}</td><td class="nowrap">{_age_cell(r)}</td>'
            f'<td class="nowrap">{_vis_cell(r)}</td><td class="nowrap">{_rain_cell(r)}</td><td class="b13">{_b13_cell(r)}</td><td>{source_status(r)}</td></tr>'
        )
    return f'<div class="tbl-wrap"><table class="tbl airports"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def metar_card(row: dict[str, Any], groups: list[dict[str, Any]]) -> str:
    icao = row.get("airport_icao", "")
    fetched = row.get("metar_fetched_at_utc")
    sub = "AviationWeather"
    if fetched:
        info = normalize_timestamp_pair(fetched)
        sub += f" · hệ thống nhận lúc {info['utc_hhmm']} ({info['vn_hhmm']} ICT)"
    if not groups:
        body = notice("Không có bản tin METAR", "Mốc này không có bản tin quan trắc; sân bay được xếp Mức 2 để xác minh.", "warn")
    else:
        toks = "".join(
            f'<span class="tok{" tok-basis" if g["is_basis"] else ""} tok-{_e(g["kind"])}" title="{_e(g["meaning_vi"])}">{_e(g["token"])}</span>'
            for g in groups
        )
        body = f'<div class="metar-raw mono">{toks}</div>'
    speci = "SPECI / bản sửa" if row.get("is_amended_or_speci") else "Bản gốc · không SPECI"
    vic = ", ".join(row.get("vicinity_weather_codes") or []) or "Không ghi nhận"
    obs = (
        f"Quan trắc {_e(row.get('obs_utc_hhmm'))} · {_e(row.get('obs_vn_hhmm'))} ICT"
        if row.get("metar_is_present", True) and row.get("obs_utc_hhmm") not in (None, "N/A") else "Quan trắc —"
    )
    meta = "".join(f"<span>{m}</span>" for m in (
        obs, f"Tuổi {_fmt_age(row.get('metar_age_minutes'))}", f"Lân cận: {_e(vic)}", _e(speci),
    ))
    return (
        f'<div class="card"><div class="card-head"><div class="card-title">METAR gốc · {_e(icao)}</div>'
        f'<div class="card-sub">{_e(sub)}</div></div>{body}<div class="metar-meta">{meta}</div></div>'
    )


def why_block(explanation: dict[str, Any]) -> str:
    items = "".join(
        f'<li class="{"met" if c["met"] else "unmet"}"><span class="chk">{"✓" if c["met"] else "✕"}</span>'
        f'<span class="why-label">{_e(c["label"])}</span><span class="why-val mono">{_e(c["value"])}</span></li>'
        for c in explanation.get("checks", [])
    )
    lv = _level(explanation.get("level"))
    return (
        f'<div class="why why-lv{lv}"><div class="why-kicker">VÌ SAO XẾP MỨC NÀY</div><ul>{items}</ul>'
        f'<div class="why-result">{level_badge(lv)}</div>'
        f'<div class="why-summary">{_e(explanation.get("summary_vi"))}</div></div>'
    )


def decode_list(groups: list[dict[str, Any]]) -> str:
    rows = "".join(
        f'<div class="dec-row{" basis" if g["is_basis"] else ""}"><span class="dec-tok mono">{_e(g["token"])}</span>'
        f'<span class="dec-txt">{_e(g["meaning_vi"])}</span>'
        f'{"<span class=" + chr(34) + "pill pill-danger" + chr(34) + ">Căn cứ</span>" if g["is_basis"] else ""}</div>'
        for g in groups
    )
    return f'<div class="dec-list">{rows}</div>'


def forecast_card(row: dict[str, Any]) -> str:
    rain = row.get("forecast_rain_next_hour_mm")
    fetched = row.get("forecast_fetched_at_utc")
    present = fetched is not None and rain is not None
    val = float(rain) if rain is not None else 0.0
    if present:
        alert = val >= FORECAST_RAIN_ALERT_MM
        f_info = normalize_timestamp_pair(fetched)
        validity = f"nhận {f_info['utc_hhmm']} ({f_info['vn_hhmm']} ICT)"
        target = row.get("target_forecast_hour_utc")
        if target:
            t_info = normalize_timestamp_pair(target)
            validity = f"Hiệu lực {t_info['utc_hhmm']} ({t_info['vn_hhmm']} ICT) · {validity}"
        status = pill(f"Vượt ngưỡng {FORECAST_RAIN_ALERT_MM} mm", "info") if alert else pill(f"Dưới ngưỡng {FORECAST_RAIN_ALERT_MM} mm", "neutral")
        note = f'<div class="card-foot">{_e(validity)}</div>'
    else:
        status = pill("Không có dự báo", "neutral")
        note = '<div class="card-foot">Mặc định 0.0 mm/h theo nguyên tắc bảo thủ (mốc lịch sử trước khi bật máy thu dự báo).</div>'
    return (
        '<div class="card"><div class="card-head"><div class="card-title">Dự báo mưa giờ tới</div>'
        '<div class="card-sub">Open-Meteo</div></div>'
        f'<div class="fc-value"><span class="fc-num mono">{val:.1f}</span><span class="fc-unit">mm/h</span>{status}</div>'
        f"{note}<div class=\"card-foot muted\">Ngưỡng cảnh báo Mức 3: ≥ {FORECAST_RAIN_ALERT_MM} mm.</div></div>"
    )


def b13_metrics(row: dict[str, Any]) -> str:
    if not row.get("b13_is_present"):
        vals = ["—", "—", "—", "—"]
    else:
        vf = row.get("b13_valid_fraction")
        bt = row.get("inner_bt_min_c")
        cold = row.get("inner_cold_cloud_fraction")
        sev = row.get("inner_severe_convection_fraction")
        vals = [
            f"{float(vf) * 100:.1f}%" if vf is not None else "—",
            f"{float(bt):.1f}°C" if bt is not None else "—",
            f"{float(cold) * 100:.1f}%" if cold is not None else "—",
            f"{float(sev) * 100:.1f}%" if sev is not None else "—",
        ]
    labels = ["Độ phủ hợp lệ", "Nhiệt độ sáng cực tiểu (BT min)", "Tỷ lệ mây lạnh < −40°C", "Tỷ lệ đối lưu mạnh < −60°C"]
    return tile_grid((stat_tile(lbl, v) for lbl, v in zip(labels, vals)), cols=2)


ATTRIBUTION = "Processed Product | Source: JMA/NOAA Himawari-9 B13 via AWS Open Data | Cropped & Annotated by Group"


def attribution() -> str:
    return f'<div class="attrib">{_e(ATTRIBUTION)}</div>'


def baseline_lock_notice() -> str:
    return (
        '<div class="lock"><div class="lock-ic">🔒</div><div class="lock-title">Tạm ẩn trong lượt thử Baseline</div>'
        '<div class="lock-body">Banner, bảng ưu tiên và màu theo mức được ẩn để không lộ đáp án. '
        "Nộp bài hoặc bấm “Đặt lại” ở Tab Đo KPI để mở lại.</div></div>"
    )


# ---------------------------------------------------------------------------
# Tab 2 — KPI
# ---------------------------------------------------------------------------

def format_clock(seconds: float) -> str:
    total = max(0, int(seconds))
    return f"{total // 60:02d}:{total % 60:02d}"


def timer_display(state: str, elapsed: float) -> str:
    labels = {"idle": "Chưa bắt đầu", "running": "Đang chạy", "stopped": "Đã dừng"}
    state = state if state in labels else "idle"
    return (
        f'<div class="timer timer-{state}"><div class="timer-state">'
        f'<span class="dot"></span>{labels[state]}</div>'
        f'<div class="timer-clock mono">{format_clock(elapsed)}</div>'
        f'<div class="timer-sub mono">{elapsed:.1f} giây</div></div>'
    )


def mini_decision_table(rows: list[dict[str, Any]]) -> str:
    head = "".join(f"<th>{h}</th>" for h in ("Mức ưu tiên", "Sân bay", "Lý do", "Giờ quan trắc", "Mưa giờ tới", "BT min"))
    body = []
    for r in rows:
        icao = r.get("airport_icao", "")
        rain = r.get("forecast_rain_next_hour_mm")
        bt = r.get("inner_bt_min_c")
        obs = r.get("obs_utc_hhmm") if r.get("metar_is_present", True) else None
        body.append(
            f'<tr><td>{level_badge(r.get("priority_level"), compact=True)}</td>'
            f'<td><span class="mono strong">{_e(icao)}</span> · {_e(AIRPORT_SHORT_NAMES.get(icao, ""))}</td>'
            f'<td class="reason">{_e(r.get("reason_vi"))}</td><td class="mono">{_e(obs)}</td>'
            f'<td class="mono">{float(rain) if rain is not None else 0.0:.1f}</td>'
            f'<td class="mono">{(f"{float(bt):.1f}°C") if bt is not None else "—"}</td></tr>'
        )
    return f'<div class="tbl-wrap"><table class="tbl"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def kpi_summary_block(summary: dict[str, Any]) -> str:
    base = summary.get("baseline_3_sources", {}) or {}
    dash = summary.get("dashboard", {}) or {}
    b_med = base.get("median_passed_seconds")
    d_med = dash.get("median_passed_seconds")
    has_both = b_med is not None and d_med is not None and b_med > 0

    if has_both:
        pct = float(summary.get("time_reduction_pct", 0.0))
        diff = pct - 30.0
        sign = "+" if diff >= 0 else "−"
        ok = bool(summary.get("meets_30_pct_target"))
        headline = (
            '<div class="kpi-head"><div class="tile-label">Rút ngắn thời gian quyết định</div>'
            f'<div class="kpi-big mono {"ok" if ok else "warn"}">{pct:.1f}%</div>'
            f'<div class="tile-sub">{sign}{abs(diff):.1f} điểm so với ngưỡng 30%</div>'
            f'{pill("ĐẠT MỤC TIÊU ≥ 30%", "ok") if ok else pill("Chưa đạt ngưỡng 30%", "warn")}</div>'
        )
        target = b_med * 0.7
        scale = max(b_med, d_med, target) or 1.0
        bars = "".join(
            f'<div class="bar-row"><span class="bar-label">{label}</span><div class="bar-track">'
            f'<div class="bar-fill {cls}" style="width:{val / scale * 100:.1f}%"></div>'
            f'<div class="bar-target" style="left:{target / scale * 100:.1f}%"></div></div>'
            f'<span class="bar-val mono">{val:.1f}</span></div>'
            for label, val, cls in (("Baseline", b_med, "base"), ("Dashboard", d_med, "dash"))
        )
        chart = (
            '<div class="kpi-chart"><div class="tile-label">Thời gian trung vị của lượt đạt (giây)</div>'
            f'<div class="tile-sub">Mục tiêu ≤ {target:.1f} s (vạch đứt)</div>{bars}</div>'
        )
    else:
        headline = (
            '<div class="kpi-head"><div class="tile-label">Rút ngắn thời gian quyết định</div>'
            '<div class="kpi-big mono muted">N/A</div>'
            '<div class="tile-sub">Cần ít nhất 1 lượt đạt ở mỗi chế độ</div></div>'
        )
        chart = '<div class="kpi-chart">' + notice("Chưa có lượt thử", "Biểu đồ sẽ hiện khi cả hai chế độ đều có lượt đạt.", "neutral") + "</div>"

    def _row(name: str, info: dict[str, Any]) -> str:
        med = info.get("median_passed_seconds")
        return (
            f"<tr><td class='strong'>{name}</td><td class='mono'>{info.get('total_trials', 0)}</td>"
            f"<td class='mono'>{info.get('passed_trials', 0)}</td><td class='mono'>{float(info.get('pass_rate_pct', 0.0)):.1f}%</td>"
            f"<td class='mono'>{f'{med:.1f}' if med is not None else 'N/A'}</td></tr>"
        )

    table = (
        '<div class="tbl-wrap"><table class="tbl"><thead><tr><th>Chế độ</th><th>Tổng lượt</th><th>Lượt đạt</th>'
        f'<th>Tỷ lệ đạt</th><th>Trung vị (s)</th></tr></thead><tbody>{_row("Baseline", base)}{_row("Dashboard", dash)}</tbody></table></div>'
    )
    return f'<div class="card"><div class="kpi-grid">{headline}{chart}</div>{table}</div>'


# ---------------------------------------------------------------------------
# Tab 3 — audit
# ---------------------------------------------------------------------------

def format_bytes(n: Any) -> str:
    try:
        size = float(n)
    except (TypeError, ValueError):
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def _int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def reconciliation_table(sources: list[dict[str, Any]]) -> str:
    head = "".join(f"<th>{h}</th>" for h in (
        "Nguồn", "Dòng đọc", "Cách ly", "Hợp lệ trước lọc trùng", "Giữ lại", "Trùng bị loại", "Phân bổ", "Trạng thái",
    ))
    body = []
    for s in sources:
        read = _int(s.get("rows_read"))
        q = _int(s.get("rows_quarantined"))
        valid = _int(s.get("rows_valid_pre_dedup"))
        kept = _int(s.get("rows_retained_valid"))
        dup = _int(s.get("rows_duplicate_dropped"))
        total = read or 1
        bar = (
            '<div class="dist">'
            f'<span class="d-kept" style="width:{kept / total * 100:.1f}%"></span>'
            f'<span class="d-dup" style="width:{dup / total * 100:.1f}%"></span>'
            f'<span class="d-q" style="width:{q / total * 100:.1f}%"></span></div>'
        )
        ok = bool(s.get("is_reconciled"))
        status = pill("BẢO TOÀN 100%", "ok") if ok else pill("LỆCH DÒNG", "danger")
        body.append(
            f'<tr><td class="mono strong">{_e(s.get("source"))}</td><td class="mono">{read:,}</td><td class="mono">{q:,}</td>'
            f'<td class="mono">{valid:,}</td><td class="mono">{kept:,}</td><td class="mono">{dup:,}</td><td>{bar}</td><td>{status}</td></tr>'
        )
    legend = (
        '<div class="dist-legend"><span><i class="d-kept"></i>Giữ lại</span><span><i class="d-dup"></i>Trùng bị loại</span>'
        '<span><i class="d-q"></i>Cách ly</span></div>'
    )
    return f'<div class="tbl-wrap"><table class="tbl"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>{legend}'


def reconciliation_mismatches(sources: list[dict[str, Any]]) -> list[str]:
    """Human-readable explanation for each source whose row counts do not reconcile."""
    out = []
    for s in sources:
        if s.get("is_reconciled", True):
            continue
        read = _int(s.get("rows_read"))
        q = _int(s.get("rows_quarantined"))
        valid = _int(s.get("rows_valid_pre_dedup"))
        kept = _int(s.get("rows_retained_valid"))
        dup = _int(s.get("rows_duplicate_dropped"))
        if read != valid + q:
            gap = read - valid - q
            out.append(f"{s.get('source')}: đọc {read:,} ≠ hợp lệ {valid:,} + cách ly {q:,} ({'thiếu' if gap > 0 else 'thừa'} {abs(gap):,})")
        else:
            gap = valid - kept - dup
            out.append(f"{s.get('source')}: hợp lệ {valid:,} ≠ giữ lại {kept:,} + trùng {dup:,} ({'thiếu' if gap > 0 else 'thừa'} {abs(gap):,})")
    return out


def data_table(columns: list[str], rows: list[list[Any]], mono_cols: Iterable[int] = ()) -> str:
    mono = set(mono_cols)
    head = "".join(f"<th>{_e(c)}</th>" for c in columns)
    body = "".join(
        "<tr>" + "".join(f'<td class="{"mono" if i in mono else ""}">{_e(v)}</td>' for i, v in enumerate(r)) + "</tr>"
        for r in rows
    )
    return f'<div class="tbl-wrap"><table class="tbl"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def binary_error_cards(errors: list[dict[str, Any]]) -> str:
    cards = "".join(
        f'<div class="err-card"><div class="err-top">{pill(e.get("error_type") or "ERROR", "danger")}'
        f'<span class="mono muted">{format_bytes(e.get("file_size_bytes"))}</span></div>'
        f'<div class="mono err-path">{_e(e.get("file_path"))}</div>'
        f'<div class="sub">{_e(e.get("error_message"))} · Giả lập: {"Có" if e.get("is_synthetic_test") else "Không"}</div></div>'
        for e in errors
    )
    return f'<div class="err-grid">{cards}</div>'
