"""Single source of truth for Level 1-4 priority thresholds, labels and Vietnamese reasons.

Pure Python (no pyspark import). The Spark engine (priority_engine.py) re-expresses the same
logic as Column expressions and imports the constants below; tests/spark/test_priority_rules_parity.py
asserts both implementations agree on a full grid of inputs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

METAR_STALE_MINUTES = 60.0
FORECAST_RAIN_ALERT_MM = 0.1
LOW_VISIBILITY_M = 5000  # same threshold as transforms.parse_metar_raw_ob (visibility_m < 5000)

LABEL_LEVEL_1 = "LEVEL_1_HAZARD_AT_STATION"
LABEL_LEVEL_2 = "LEVEL_2_DATA_VERIFICATION"
LABEL_LEVEL_3 = "LEVEL_3_FORECAST_RAIN_ALERT"
LABEL_LEVEL_4 = "LEVEL_4_ROUTINE"

REASON_LEVEL_4 = "Điều kiện tại trạm bình thường, không có cảnh báo mưa giờ tới"
REASON_LEVEL_2_MISSING = "Cần xác minh dữ liệu: Thiếu bản tin quan trắc METAR"


@dataclass(frozen=True)
class PriorityInput:
    metar_is_present: bool
    metar_age_minutes: Optional[float]
    has_precip_or_thunder_at_station: bool
    is_low_visibility: bool
    station_weather_codes: Sequence[str]
    visibility_m: Optional[int]
    observed_hhmm: str  # e.g. "08:00Z"
    forecast_is_present: bool
    forecast_rain_next_hour_mm: Optional[float]
    target_forecast_hhmm: str  # e.g. "09:00Z"


@dataclass(frozen=True)
class PriorityDecision:
    level: int
    label: str
    reason_vi: str


def evaluate_priority(inp: PriorityInput) -> PriorityDecision:
    """Applies Level 1-4 rules (first match wins) and builds the Vietnamese reason."""
    fresh = inp.metar_is_present and inp.metar_age_minutes is not None and inp.metar_age_minutes <= METAR_STALE_MINUTES
    is_l1 = fresh and (inp.has_precip_or_thunder_at_station or inp.is_low_visibility)
    is_l2 = (not is_l1) and ((not inp.metar_is_present) or (inp.metar_age_minutes is not None and inp.metar_age_minutes > METAR_STALE_MINUTES))
    rain = inp.forecast_rain_next_hour_mm
    is_l3 = (not is_l1) and (not is_l2) and inp.forecast_is_present and rain is not None and rain >= FORECAST_RAIN_ALERT_MM

    if is_l1:
        wx = ", ".join(inp.station_weather_codes)
        vis = inp.visibility_m
        if inp.has_precip_or_thunder_at_station and inp.is_low_visibility:
            reason = f"METAR mới ({inp.observed_hhmm}) ghi nhận mưa/dông tại trạm ({wx}) và tầm nhìn thấp {vis}m (< 5km)"
        elif inp.has_precip_or_thunder_at_station:
            vis_txt = str(vis) if vis is not None else "không rõ"
            reason = f"METAR mới ({inp.observed_hhmm}) ghi nhận mưa/dông tại trạm ({wx}), tầm nhìn {vis_txt}m"
        else:
            reason = f"METAR mới ghi nhận tầm nhìn thấp {vis}m (< 5km)"
        return PriorityDecision(1, LABEL_LEVEL_1, reason)

    if is_l2:
        if not inp.metar_is_present:
            reason = REASON_LEVEL_2_MISSING
        else:
            reason = f"Cần xác minh dữ liệu: Bản tin METAR đã cũ {inp.metar_age_minutes} phút (> 60 phút)"
        return PriorityDecision(2, LABEL_LEVEL_2, reason)

    if is_l3:
        reason = (
            f"Điều kiện tại trạm tốt, nhưng dự báo mưa giờ tới đạt "
            f"{round(float(rain), 1)} mm (hiệu lực {inp.target_forecast_hhmm})"
        )
        return PriorityDecision(3, LABEL_LEVEL_3, reason)

    return PriorityDecision(4, LABEL_LEVEL_4, REASON_LEVEL_4)
