"""HTML builders used by the dashboard (pure functions, no Streamlit)."""

from src.dashboard import components as c


def _row(icao, level=4, **kw):
    base = {
        "airport_icao": icao, "priority_level": level, "reason_vi": "Lý do", "obs_utc_hhmm": "07:00Z",
        "obs_vn_hhmm": "14:00", "metar_is_present": True, "metar_age_minutes": 12.0, "visibility_m": 8000,
        "forecast_rain_next_hour_mm": 0.0, "b13_is_present": True, "b13_summary_vi": "Mây", "inner_bt_min_c": -20.0,
        "accepted_first_review_airports": [icao],
    }
    base.update(kw)
    return base


def test_theme_css_is_loaded_into_a_style_tag():
    tag = c.theme_style_tag()
    assert tag.startswith("<style>") and tag.endswith("</style>")
    assert "--l1: #B42318" in tag


def test_level_badges_carry_number_label_and_shape_for_all_levels():
    for lv, label in c.LEVEL_LABELS.items():
        html = c.level_badge(lv)
        assert f"lv{lv}" in html and f"Mức {lv}" in html and label in html and c.LEVEL_ICONS[lv] in html
    assert "lv4" in c.level_badge(None)  # unknown level degrades to routine, never crashes


def test_external_text_is_escaped():
    row = _row("VVDN", 1, reason_vi="<script>alert(1)</script>")
    html = c.airport_table([row])
    assert "<script>" not in html and "&lt;script&gt;" in html
    groups = [{"token": "<b>", "meaning_vi": "x", "kind": "unknown", "is_basis": False}]
    card = c.metar_card(row, groups)
    assert "<b>" not in card and "&lt;b&gt;" in card


def test_banner_single_airport():
    rows = [_row("VVDN", 1, reason_vi="Mưa tại trạm"), _row("VVNB", 3), _row("VVTS", 2)]
    html = c.decision_banner(rows)
    assert "SÂN BAY CẦN RÀ SOÁT ĐẦU TIÊN" in html and "VVDN" in html and "Mưa tại trạm" in html
    assert "banner-lv1" in html and "ĐỒNG MỨC" not in html


def test_banner_co_priority_groups_two_and_three_airports():
    two = [_row(i, 1, accepted_first_review_airports=["VVDN", "VVNB"]) for i in ("VVDN", "VVNB")] + [_row("VVTS", 4, accepted_first_review_airports=["VVDN", "VVNB"])]
    html = c.decision_banner(two)
    assert "ĐỒNG MỨC ƯU TIÊN ĐẦU" in html and html.count('class="eq"') == 1
    three = [_row(i, 4, accepted_first_review_airports=["VVDN", "VVNB", "VVTS"]) for i in ("VVDN", "VVNB", "VVTS")]
    html3 = c.decision_banner(three)
    assert html3.count('class="eq"') == 2 and "3 sân bay cùng Mức 4" in html3


def test_table_cell_states():
    missing = _row("VVTS", 2, metar_is_present=False, metar_age_minutes=None, visibility_m=None, obs_utc_hhmm="N/A")
    stale = _row("VVNB", 2, metar_age_minutes=95.0, b13_is_present=False)
    capped = _row("VVDN", 4, visibility_m=10000, visibility_capped_9999=True)
    html = c.airport_table([missing, stale, capped], selected_icao="VVNB")
    assert "N/A" in html
    assert '<span class="mono warn">95 phút</span>' in html
    assert "Khuyết nguồn" in html and "thiếu METAR" in html and "thiếu B13" in html
    assert "Đủ 3 nguồn" in html
    assert "9999m" in html and "(≥9999)" in html
    assert 'class="row-lv2 sel"' in html


def test_forecast_card_present_and_missing():
    present = c.forecast_card(_row("VVNB", 3, forecast_rain_next_hour_mm=1.2, forecast_fetched_at_utc="2026-11-09T06:58:00Z",
                                   target_forecast_hour_utc="2026-11-09T08:00:00Z"))
    assert "1.2" in present and "Vượt ngưỡng" in present and "Hiệu lực 08:00Z" in present
    missing = c.forecast_card(_row("VVDN", 1, forecast_rain_next_hour_mm=None, forecast_fetched_at_utc=None))
    assert "0.0" in missing and "nguyên tắc bảo thủ" in missing


def test_b13_metrics_show_dash_without_image():
    html = c.b13_metrics(_row("VVTS", 2, b13_is_present=False))
    assert html.count(">—<") == 4


def test_timer_and_clock():
    assert c.format_clock(115.8) == "01:55"
    assert "Đang chạy" in c.timer_display("running", 42.0) and "00:42" in c.timer_display("running", 42.0)
    assert "Chưa bắt đầu" in c.timer_display("idle", 0.0)


def test_kpi_summary_states():
    empty = c.kpi_summary_block({"baseline_3_sources": {}, "dashboard": {}})
    assert "N/A" in empty and "Chưa có lượt thử" in empty
    full = c.kpi_summary_block({
        "baseline_3_sources": {"total_trials": 6, "passed_trials": 5, "pass_rate_pct": 83.3, "median_passed_seconds": 74.0},
        "dashboard": {"total_trials": 6, "passed_trials": 6, "pass_rate_pct": 100.0, "median_passed_seconds": 31.5},
        "time_reduction_pct": 57.4, "meets_30_pct_target": True,
    })
    assert "57.4%" in full and "+27.4 điểm" in full and "ĐẠT MỤC TIÊU" in full and "51.8" in full


def test_reconciliation_table_and_mismatch_text():
    ok = {"source": "metar", "rows_read": 10, "rows_quarantined": 1, "rows_valid_pre_dedup": 9,
          "rows_retained_valid": 7, "rows_duplicate_dropped": 2, "is_reconciled": True}
    bad = {"source": "open_meteo", "rows_read": 2160, "rows_quarantined": 0, "rows_valid_pre_dedup": 2150,
           "rows_retained_valid": 2150, "rows_duplicate_dropped": 0, "is_reconciled": False}
    html = c.reconciliation_table([ok, bad])
    assert "BẢO TOÀN 100%" in html and "LỆCH DÒNG" in html
    assert c.reconciliation_mismatches([ok, bad]) == ["open_meteo: đọc 2,160 ≠ hợp lệ 2,150 + cách ly 0 (thiếu 10)"]


def test_format_bytes():
    assert c.format_bytes(0) == "0 B"
    assert c.format_bytes(2150) == "2.10 KB"
    assert c.format_bytes(1975610368) == "1.84 GB"
