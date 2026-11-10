"""METAR group decoder and the "Vì sao xếp mức này" explanation."""

from src.dashboard.metar_decoder import decode_metar, describe_weather, explain_priority
from src.spark import priority_rules

SAMPLE = "VVDN 090700Z 36005KT 3000 RA BKN015 FEW020CB 24/23 Q1012 TEMPO 2000 TSRA"


def test_design_sample_decodes_into_ten_groups():
    groups = decode_metar(SAMPLE, "VVDN")
    assert [g["token"] for g in groups] == [
        "VVDN", "090700Z", "36005KT", "3000", "RA", "BKN015", "FEW020CB", "24/23", "Q1012", "TEMPO 2000 TSRA",
    ]
    meaning = {g["token"]: g["meaning_vi"] for g in groups}
    assert meaning["VVDN"] == "Mã ICAO · Sân bay Đà Nẵng"
    assert meaning["090700Z"] == "Ngày 09 · quan trắc lúc 07:00Z (14:00 ICT)"
    assert meaning["36005KT"] == "Gió hướng 360°, 5 kt"
    assert meaning["3000"] == "Tầm nhìn 3000 m — dưới ngưỡng 5 km"
    assert meaning["RA"] == "Hiện tượng tại trạm: mưa"
    assert meaning["BKN015"] == "Mây nhiều (5–7/8) ở 1500 ft"
    assert meaning["FEW020CB"] == "Ít mây (1–2/8) ở 2000 ft · mây vũ tích (CB)"
    assert meaning["24/23"] == "Nhiệt độ 24°C · điểm sương 23°C"
    assert meaning["Q1012"] == "Khí áp QNH 1012 hPa"
    assert meaning["TEMPO 2000 TSRA"] == "Tạm thời: tầm nhìn 2000 m, dông kèm mưa"


def test_basis_groups_are_station_precip_and_low_visibility_only():
    groups = decode_metar(SAMPLE, "VVDN")
    assert {g["token"] for g in groups if g["is_basis"]} == {"3000", "RA"}


def test_real_feed_prefix_and_variants():
    raw = "METAR VVDN 090800Z 11013KT 070V150 8000 -RA SCT017 FEW023TCU SCT050 28/24 Q1016 NOSIG"
    groups = decode_metar(raw, "VVDN")
    tokens = [g["token"] for g in groups]
    assert tokens[0] == "VVDN"  # METAR report-type prefix dropped
    meaning = {g["token"]: g["meaning_vi"] for g in groups}
    assert meaning["070V150"] == "Hướng gió thay đổi từ 70° đến 150°"
    assert meaning["-RA"] == "Hiện tượng tại trạm: mưa nhẹ"
    assert "mây tích phát triển (TCU)" in meaning["FEW023TCU"]
    assert meaning["8000"] == "Tầm nhìn 8000 m"
    assert meaning["NOSIG"].startswith("Không có thay đổi")
    assert {g["token"] for g in groups if g["is_basis"]} == {"-RA"}


def test_empty_and_unknown_tokens():
    assert decode_metar(None) == []
    assert decode_metar("   ") == []
    groups = decode_metar("VVTS 090700Z ZZZ9 9999 M02/M05")
    by_tok = {g["token"]: g for g in groups}
    assert by_tok["ZZZ9"]["meaning_vi"] == "Nhóm chưa giải mã"
    assert by_tok["9999"]["meaning_vi"] == "Tầm nhìn ≥ 10 km"
    assert by_tok["M02/M05"]["meaning_vi"] == "Nhiệt độ -2°C · điểm sương -5°C"


def test_weather_phrases():
    assert describe_weather("TSRA") == "dông kèm mưa"
    assert describe_weather("SHRA") == "mưa rào"
    assert describe_weather("BR") == "mù"
    assert describe_weather("VCTS") == "dông lân cận"


def test_explain_level1_matches_design():
    row = {
        "priority_level": 1, "metar_is_present": True, "raw_ob": SAMPLE, "metar_age_minutes": 12.0,
        "has_precip_or_thunder_at_station": True, "station_weather_codes": ["RA"],
        "visibility_m": 3000, "is_low_visibility": True,
    }
    exp = explain_priority(row)
    assert exp["label_vi"] == "Nguy hiểm tại trạm"
    assert [(c["label"], c["value"], c["met"]) for c in exp["checks"]] == [
        ("Bản tin còn mới (≤ 60 phút)", "12 phút", True),
        ("Mưa/dông tại trạm", "Có · RA", True),
        ("Tầm nhìn < 5 km", "3000 m", True),
    ]
    assert exp["summary_vi"] == "Bản tin còn mới và có mưa/dông tại trạm và tầm nhìn dưới 5 km, nên dừng ở Mức 1."


def test_explain_level2_missing_and_stale():
    missing = explain_priority({"priority_level": 2, "metar_is_present": False, "raw_ob": None})
    assert missing["checks"] == [{"label": "Có bản tin METAR", "value": "Không có", "met": False}]
    stale = explain_priority({"priority_level": 2, "metar_is_present": True, "raw_ob": "x", "metar_age_minutes": 95})
    assert stale["checks"][0]["met"] is False
    assert "95 phút" in stale["summary_vi"]


def test_explain_level3_includes_forecast_check():
    row = {
        "priority_level": 3, "metar_is_present": True, "raw_ob": "x", "metar_age_minutes": 8,
        "has_precip_or_thunder_at_station": False, "visibility_m": 8000,
        "forecast_is_present": True, "forecast_rain_next_hour_mm": 1.2,
    }
    exp = explain_priority(row)
    assert exp["checks"][-1] == {"label": "Dự báo mưa giờ tới ≥ 0.1 mm", "value": "1.2 mm", "met": True}


def test_thresholds_come_from_priority_rules():
    assert priority_rules.LOW_VISIBILITY_M == 5000
    assert priority_rules.METAR_STALE_MINUTES == 60.0
