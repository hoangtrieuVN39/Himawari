"""Decodes a raw METAR into token groups with Vietnamese explanations, and explains the priority level.

Pure Python (no Streamlit). Thresholds come from src.spark.priority_rules so the "why" block can never
disagree with the rules that actually produced the level.
"""

from __future__ import annotations

import re
from typing import Any

from src.spark.priority_rules import FORECAST_RAIN_ALERT_MM, LOW_VISIBILITY_M, METAR_STALE_MINUTES

AIRPORT_LABELS: dict[str, str] = {
    "VVDN": "Sân bay Đà Nẵng",
    "VVNB": "Sân bay Nội Bài",
    "VVTS": "Sân bay Tân Sơn Nhất",
}

LEVEL_LABELS: dict[int, str] = {
    1: "Nguy hiểm tại trạm",
    2: "Cần xác minh dữ liệu",
    3: "Cảnh báo mưa dự báo",
    4: "Bình thường",
}

# Same grammar as src/spark/transforms.py (station precipitation / thunder vs vicinity weather).
_PRECIP_TS = re.compile(r"^(\+|-)?(TS|SH|FZ|BL|DR)?(RA|DZ|SN|SG|IC|PL|GR|GS|UP|SQ|FC|TS)+$")
_WX = re.compile(r"^(\+|-|VC)?(MI|BC|PR|DR|BL|SH|TS|FZ)?((?:RA|DZ|SN|SG|IC|PL|GR|GS|UP|BR|FG|FU|VA|DU|SA|HZ|PO|SQ|FC|SS|DS)+)?$")
_WIND = re.compile(r"^(\d{3}|VRB)(\d{2,3})(?:G(\d{2,3}))?(KT|MPS)$")
_WIND_VAR = re.compile(r"^(\d{3})V(\d{3})$")
_TIME = re.compile(r"^(\d{2})(\d{2})(\d{2})Z$")
_VIS = re.compile(r"^(\d{4})(N|NE|E|SE|S|SW|W|NW|NDV)?$")
_CLOUD = re.compile(r"^(FEW|SCT|BKN|OVC|VV)(\d{3}|///)(CB|TCU)?$")
_TEMP = re.compile(r"^(M?\d{2})/(M?\d{2})?$")
_QNH = re.compile(r"^Q(\d{4})$")
_ALT = re.compile(r"^A(\d{4})$")
_RVR = re.compile(r"^R(\d{2}[LCR]?)/([PM]?\d{4})")

_INTENSITY = {"-": "nhẹ", "+": "mạnh"}
_DESCRIPTOR = {
    "MI": "mỏng", "BC": "từng đám", "PR": "một phần", "DR": "cuốn thấp", "BL": "cuốn cao",
    "SH": "mưa rào", "TS": "dông", "FZ": "đóng băng",
}
_PHENOMENA = {
    "RA": "mưa", "DZ": "mưa phùn", "SN": "tuyết", "SG": "tuyết hạt", "IC": "tinh thể băng",
    "PL": "băng viên", "GR": "mưa đá", "GS": "mưa đá nhỏ", "UP": "giáng thủy không xác định",
    "BR": "mù", "FG": "sương mù", "FU": "khói", "VA": "tro núi lửa", "DU": "bụi", "SA": "cát",
    "HZ": "mù khô", "PO": "lốc bụi", "SQ": "gió giật mạnh (squall)", "FC": "vòi rồng",
    "SS": "bão cát", "DS": "bão bụi",
}
_COVER = {
    "FEW": "Ít mây (1–2/8)", "SCT": "Mây rải rác (3–4/8)", "BKN": "Mây nhiều (5–7/8)",
    "OVC": "Mây che kín (8/8)", "VV": "Tầm nhìn thẳng đứng",
}
_CLOUD_TYPE = {"CB": "mây vũ tích (CB)", "TCU": "mây tích phát triển (TCU)"}
_SIMPLE = {
    "CAVOK": "Trời quang, tầm nhìn ≥ 10 km, không mây dưới 5000 ft",
    "NSC": "Không có mây đáng kể",
    "NCD": "Không phát hiện mây",
    "SKC": "Trời quang",
    "CLR": "Không có mây dưới 12000 ft",
    "NOSIG": "Không có thay đổi đáng kể trong 2 giờ tới",
    "AUTO": "Bản tin tự động",
    "COR": "Bản tin đã sửa (COR)",
    "NIL": "Không có bản tin",
}


def _hhmm_ict(hh: int, mm: int) -> str:
    return f"{(hh + 7) % 24:02d}:{mm:02d}"


def describe_weather(code: str) -> str | None:
    """Vietnamese phrase for a present-weather code (e.g. "-RA" -> "mưa nhẹ", "TSRA" -> "dông kèm mưa")."""
    m = _WX.match(code)
    if not m or not (m.group(2) or m.group(3)):
        return None
    prefix, desc, phen = m.group(1), m.group(2), m.group(3) or ""
    phen_words = [_PHENOMENA[phen[i:i + 2]] for i in range(0, len(phen), 2) if phen[i:i + 2] in _PHENOMENA]
    if desc == "TS":
        text = "dông" + (" kèm " + ", ".join(phen_words) if phen_words else "")
    elif desc == "SH":
        text = "mưa rào" if phen_words in ([], ["mưa"]) else "mưa rào " + ", ".join(phen_words)
    elif desc:
        text = ", ".join(phen_words) + " " + _DESCRIPTOR[desc] if phen_words else _DESCRIPTOR[desc]
    else:
        text = ", ".join(phen_words)
    if prefix == "VC":
        return f"{text} lân cận"
    if prefix in _INTENSITY:
        text = f"{text} {_INTENSITY[prefix]}" if desc != "TS" else f"{text} ({_INTENSITY[prefix]})"
    return text


def _visibility_text(tok: str) -> str | None:
    m = _VIS.match(tok)
    if not m:
        return None
    val = int(m.group(1))
    if val >= 9999:
        return "Tầm nhìn ≥ 10 km"
    text = f"Tầm nhìn {val} m"
    if m.group(2) and m.group(2) != "NDV":
        text += f" (hướng {m.group(2)})"
    if val < LOW_VISIBILITY_M:
        text += f" — dưới ngưỡng {LOW_VISIBILITY_M // 1000} km"
    return text


def _decode_token(tok: str, icao: str | None) -> tuple[str | None, str]:
    """Returns (meaning_vi, kind) for one main-body token; meaning None when unknown."""
    if icao and tok == icao or (re.fullmatch(r"[A-Z]{4}", tok) and tok in AIRPORT_LABELS):
        return f"Mã ICAO · {AIRPORT_LABELS.get(tok, 'Sân bay ' + tok)}", "station"
    if tok in _SIMPLE:
        return _SIMPLE[tok], "info"
    m = _TIME.match(tok)
    if m:
        d, hh, mm = (int(x) for x in m.groups())
        return f"Ngày {d:02d} · quan trắc lúc {hh:02d}:{mm:02d}Z ({_hhmm_ict(hh, mm)} ICT)", "time"
    m = _WIND.match(tok)
    if m:
        direction, speed, gust, unit = m.groups()
        unit_txt = "kt" if unit == "KT" else "m/s"
        if direction == "000" and int(speed) == 0:
            return "Lặng gió", "wind"
        dir_txt = "Gió đổi hướng" if direction == "VRB" else f"Gió hướng {int(direction)}°"
        text = f"{dir_txt}, {int(speed)} {unit_txt}"
        if gust:
            text += f", giật {int(gust)} {unit_txt}"
        return text, "wind"
    m = _WIND_VAR.match(tok)
    if m:
        return f"Hướng gió thay đổi từ {int(m.group(1))}° đến {int(m.group(2))}°", "wind"
    vis = _visibility_text(tok)
    if vis:
        return vis, "visibility"
    m = _RVR.match(tok)
    if m:
        return f"Tầm nhìn đường băng {m.group(1)}: {m.group(2).lstrip('PM')} m", "visibility"
    m = _CLOUD.match(tok)
    if m:
        cover, height, ctype = m.groups()
        h_txt = "không rõ độ cao" if height == "///" else f"ở {int(height) * 100} ft"
        text = f"{_COVER[cover]} {h_txt}"
        if ctype:
            text += f" · {_CLOUD_TYPE[ctype]}"
        return text, "cloud"
    m = _TEMP.match(tok)
    if m:
        t = m.group(1).replace("M", "-")
        text = f"Nhiệt độ {int(t)}°C"
        if m.group(2):
            text += f" · điểm sương {int(m.group(2).replace('M', '-'))}°C"
        return text, "temp"
    m = _QNH.match(tok)
    if m:
        return f"Khí áp QNH {int(m.group(1))} hPa", "pressure"
    m = _ALT.match(tok)
    if m:
        return f"Khí áp {int(m.group(1)) / 100:.2f} inHg", "pressure"
    wx = describe_weather(tok)
    if wx:
        if tok.startswith("VC"):
            return f"Hiện tượng lân cận: {wx.removesuffix(' lân cận')}", "vicinity"
        return f"Hiện tượng tại trạm: {wx}", "weather"
    return None, "unknown"


def _describe_trend(tokens: list[str]) -> str:
    head = tokens[0]
    parts: list[str] = []
    for tok in tokens[1:]:
        if re.fullmatch(r"(FM|TL|AT)\d{4}", tok):
            label = {"FM": "từ", "TL": "đến", "AT": "lúc"}[tok[:2]]
            parts.append(f"{label} {tok[2:4]}:{tok[4:]}Z")
            continue
        vis = _VIS.match(tok)
        if vis:
            val = int(vis.group(1))
            parts.append("tầm nhìn ≥ 10 km" if val >= 9999 else f"tầm nhìn {val} m")
            continue
        wx = describe_weather(tok)
        if wx:
            parts.append(wx)
            continue
        meaning, _ = _decode_token(tok, None)
        parts.append(meaning[0].lower() + meaning[1:] if meaning else tok)
    lead = "Tạm thời" if head == "TEMPO" else "Dần chuyển"
    return f"{lead}: {', '.join(parts)}" if parts else lead


def decode_metar(raw_ob: str | None, airport_icao: str | None = None) -> list[dict[str, Any]]:
    """Splits a raw METAR into display groups.

    Each group: {"token", "meaning_vi", "kind", "is_basis"}. The "METAR"/"SPECI" report-type prefix is
    dropped (it is not a weather group). TEMPO/BECMG groups and RMK are kept as single groups. Tokens
    the decoder does not know are returned verbatim with meaning "Nhóm chưa giải mã" so nothing is hidden.
    is_basis marks the groups that can trigger Level 1: station precipitation/thunder and visibility
    below LOW_VISIBILITY_M in the main body (trend groups never count, matching the Spark parser).
    """
    if not raw_ob or not str(raw_ob).strip():
        return []
    tokens = str(raw_ob).split()
    if tokens and tokens[0] in ("METAR", "SPECI"):
        tokens = tokens[1:]

    groups: list[dict[str, Any]] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok in ("TEMPO", "BECMG"):
            j = i + 1
            while j < len(tokens) and tokens[j] not in ("TEMPO", "BECMG", "RMK", "NOSIG"):
                j += 1
            chunk = tokens[i:j]
            groups.append({"token": " ".join(chunk), "meaning_vi": _describe_trend(chunk), "kind": "trend", "is_basis": False})
            i = j
            continue
        if tok == "RMK":
            rest = " ".join(tokens[i:])
            groups.append({"token": rest, "meaning_vi": "Ghi chú bổ sung của trạm", "kind": "remark", "is_basis": False})
            break
        meaning, kind = _decode_token(tok, airport_icao)
        is_basis = False
        if kind == "weather" and _PRECIP_TS.match(tok):
            is_basis = True
        elif kind == "visibility" and _VIS.match(tok) and int(tok[:4]) < LOW_VISIBILITY_M:
            is_basis = True
        groups.append({
            "token": tok,
            "meaning_vi": meaning or "Nhóm chưa giải mã",
            "kind": kind,
            "is_basis": is_basis,
        })
        i += 1
    return groups


def explain_priority(row: dict[str, Any]) -> dict[str, Any]:
    """Builds the "Vì sao xếp mức này" checklist for one decision-table row.

    Returns {"level", "label_vi", "checks": [{"label", "value", "met"}], "summary_vi"}. The checks follow
    the first-match rule order of priority_rules.evaluate_priority and stop at the level that matched.
    """
    level = int(row.get("priority_level") or 4)
    present = bool(row.get("metar_is_present", True))  # same presence rule as priority_rules.evaluate_priority
    age = row.get("metar_age_minutes")
    fresh = present and age is not None and float(age) <= METAR_STALE_MINUTES
    precip = bool(row.get("has_precip_or_thunder_at_station"))
    codes = row.get("station_weather_codes") or []
    vis = row.get("visibility_m")
    low_vis = bool(row.get("is_low_visibility")) or (vis is not None and vis < LOW_VISIBILITY_M)
    rain = row.get("forecast_rain_next_hour_mm")
    rain_alert = bool(row.get("forecast_is_present")) and rain is not None and float(rain) >= FORECAST_RAIN_ALERT_MM

    age_txt = f"{int(round(float(age)))} phút" if age is not None else "—"
    vis_txt = "≥ 9999 m" if row.get("visibility_capped_9999") else (f"{vis} m" if vis is not None else "không rõ")
    stale = int(METAR_STALE_MINUTES)

    checks: list[dict[str, Any]] = []
    if not present:
        checks.append({"label": "Có bản tin METAR", "value": "Không có", "met": False})
    else:
        checks.append({"label": f"Bản tin còn mới (≤ {stale} phút)", "value": age_txt, "met": fresh})
    if level != 2 and present:
        checks.append({"label": "Mưa/dông tại trạm", "value": ("Có · " + ", ".join(codes)) if precip else "Không", "met": precip})
        checks.append({"label": f"Tầm nhìn < {LOW_VISIBILITY_M // 1000} km", "value": vis_txt, "met": low_vis})
    if level >= 3:
        rain_txt = f"{float(rain):.1f} mm" if rain is not None else "Không có dự báo"
        checks.append({"label": f"Dự báo mưa giờ tới ≥ {FORECAST_RAIN_ALERT_MM} mm", "value": rain_txt, "met": rain_alert})

    if level == 1:
        causes = [c for c, on in (("có mưa/dông tại trạm", precip), (f"tầm nhìn dưới {LOW_VISIBILITY_M // 1000} km", low_vis)) if on]
        summary = f"Bản tin còn mới và {' và '.join(causes) or 'có hiện tượng nguy hiểm'}, nên dừng ở Mức 1."
    elif level == 2:
        summary = (
            "Không có bản tin METAR cho mốc này, nên cần xác minh dữ liệu (Mức 2)."
            if not present
            else f"Bản tin đã cũ {age_txt} (> {stale} phút), nên cần xác minh dữ liệu (Mức 2)."
        )
    elif level == 3:
        summary = "Trạm không có hiện tượng nguy hiểm nhưng dự báo có mưa giờ tới, nên dừng ở Mức 3."
    else:
        summary = "Không điều kiện nào ở Mức 1–3 thỏa, nên xếp Mức 4 (bình thường)."

    return {"level": level, "label_vi": LEVEL_LABELS.get(level, ""), "checks": checks, "summary_vi": summary}
