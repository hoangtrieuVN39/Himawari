"""KPI auto-grading, time matching, persistence, and median KPI statistics."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import statistics
from typing import Any
import uuid

from src.dashboard.data_loader import normalize_timestamp_pair

REASON_OPTIONS: dict[str, str] = {
    "PRECIP_OR_THUNDER_AT_STATION": "Mưa/dông ghi nhận tại trạm (METAR)",
    "LOW_VISIBILITY": "Tầm nhìn giảm thấp (< 5000m)",
    "STALE_OR_MISSING_METAR": "Bản tin quan trắc bị thiếu hoặc quá cũ (> 60 phút)",
    "FORECAST_RAIN_NEXT_HOUR": "Dự báo có mưa trong giờ tới (>= 0.1 mm/h)",
    "ROUTINE": "Thời tiết bình thường / Giám sát định kỳ",
}


def derive_expected_reason_code(airport_row: dict[str, Any]) -> str:
    """Determine expected canonical reason code based on data flags."""
    metar_present = airport_row.get("metar_is_present", True)
    metar_age = airport_row.get("metar_age_minutes")
    has_freshness_field = "metar_is_present" in airport_row or "metar_age_minutes" in airport_row
    is_stale_or_missing = (not metar_present) or (metar_age is not None and metar_age > 60.0)

    # When freshness fields are specified, stale or missing METAR takes precedence over weather flags
    if has_freshness_field and is_stale_or_missing:
        return "STALE_OR_MISSING_METAR"

    # Fresh METAR hazard flags
    if airport_row.get("has_precip_or_thunder_at_station"):
        return "PRECIP_OR_THUNDER_AT_STATION"
    if airport_row.get("is_low_visibility") or (
        airport_row.get("visibility_m") is not None and airport_row.get("visibility_m") < 5000
    ):
        return "LOW_VISIBILITY"
    if is_stale_or_missing:
        return "STALE_OR_MISSING_METAR"

    forecast_rain = airport_row.get("forecast_rain_next_hour_mm")
    if forecast_rain is not None and forecast_rain >= 0.1:
        return "FORECAST_RAIN_NEXT_HOUR"
    return "ROUTINE"


def matches_observation_time(entered_str: str, airport_row: dict[str, Any]) -> bool:
    """Check if entered observation time matches expected UTC or Vietnam time (ICT).
    
    Strictly respects explicit timezone suffixes:
        - "Z" or "UTC" suffixes ONLY match UTC observation time.
        - "ICT" or "VN" suffixes ONLY match Vietnam (UTC+7) observation time.
        - Suffix-less time strings accept either UTC or ICT time.
        - 6-digit METAR code (DDHHMM[Z]) must strictly match day, hour, and minute in UTC.
    """
    if not entered_str or not isinstance(entered_str, str):
        return False

    s = entered_str.strip().upper()
    if not s:
        return False

    utc_hhmm = airport_row.get("obs_utc_hhmm")
    vn_hhmm = airport_row.get("obs_vn_hhmm")
    ddhhmmz = airport_row.get("obs_ddhhmmz")

    if not (utc_hhmm and vn_hhmm):
        info = normalize_timestamp_pair(
            airport_row.get("observed_at_utc") or airport_row.get("review_slot_utc"),
            airport_row.get("raw_ob"),
        )
        utc_hhmm = info["utc_hhmm"]
        vn_hhmm = info["vn_hhmm"]
        ddhhmmz = info.get("metar_ddhhmmz", "")

    utc_clean = utc_hhmm.replace("Z", "")
    utc_h, utc_m = [int(x) for x in utc_clean.split(":")]
    vn_h, vn_m = [int(x) for x in vn_hhmm.split(":")]

    expected_day: int | None = None
    if ddhhmmz and len(ddhhmmz) >= 6 and ddhhmmz[:2].isdigit():
        expected_day = int(ddhhmmz[:2])

    def _match_time_and_tz(h: int, m: int, tz_suffix: str | None) -> bool:
        if not (0 <= h <= 23 and 0 <= m <= 59):
            return False
        if tz_suffix in ("Z", "UTC"):
            return h == utc_h and m == utc_m
        if tz_suffix in ("ICT", "VN"):
            return h == vn_h and m == vn_m
        return (h == utc_h and m == utc_m) or (h == vn_h and m == vn_m)

    # 1. 6-digit METAR code: DDHHMM or DDHHMMZ (explicitly UTC)
    m_ddhhmm = re.match(r"^(\d{2})(\d{2})(\d{2})\s*(Z|UTC)?$", s)
    if m_ddhhmm:
        d = int(m_ddhhmm.group(1))
        h = int(m_ddhhmm.group(2))
        m = int(m_ddhhmm.group(3))
        if not (1 <= d <= 31 and 0 <= h <= 23 and 0 <= m <= 59):
            return False
        if expected_day is not None and d != expected_day:
            return False
        return h == utc_h and m == utc_m

    # 2. HH:MM with optional suffix (e.g., "07:00Z", "07:00 UTC", "14:00 ICT", "14h00 VN", "07:00", "7:00")
    m_colon = re.match(r"^(\d{1,2}):(\d{2})\s*(Z|UTC|ICT|VN)?$", s)
    if m_colon:
        h = int(m_colon.group(1))
        m = int(m_colon.group(2))
        tz_suffix = m_colon.group(3)
        return _match_time_and_tz(h, m, tz_suffix)

    # 3. 4-digit HHMM with optional suffix (e.g., "0700Z", "0700 UTC", "1400", "1400 ICT")
    m_4digit = re.match(r"^(\d{2})(\d{2})\s*(Z|UTC|ICT|VN)?$", s)
    if m_4digit:
        h = int(m_4digit.group(1))
        m = int(m_4digit.group(2))
        tz_suffix = m_4digit.group(3)
        return _match_time_and_tz(h, m, tz_suffix)

    # 4. H format with minutes (e.g., "14h00", "14H00 VN", "7h00 ICT")
    m_h_min = re.match(r"^(\d{1,2})H(\d{2})\s*(Z|UTC|ICT|VN)?$", s)
    if m_h_min:
        h = int(m_h_min.group(1))
        m = int(m_h_min.group(2))
        tz_suffix = m_h_min.group(3)
        return _match_time_and_tz(h, m, tz_suffix)

    # 5. Hour only (e.g., "14h", "14H VN", "14", "7h", "7h00") -> minute must be 0
    m_hour_only = re.match(r"^(\d{1,2})\s*H?\s*(Z|UTC|ICT|VN)?$", s)
    if m_hour_only:
        h = int(m_hour_only.group(1))
        tz_suffix = m_hour_only.group(2)
        if not (0 <= h <= 23):
            return False
        if tz_suffix in ("Z", "UTC"):
            return utc_m == 0 and h == utc_h
        if tz_suffix in ("ICT", "VN"):
            return vn_m == 0 and h == vn_h
        return (utc_m == 0 and h == utc_h) or (vn_m == 0 and h == vn_h)

    return False


def evaluate_kpi_submission(
    slot_rows: list[dict[str, Any]],
    selected_airport: str,
    selected_reason_code: str,
    entered_obs_time: str,
    elapsed_seconds: float,
    status: str = "COMPLETED",
) -> dict[str, Any]:
    """Auto-grade a KPI trial submission against ground truth for the review slot."""
    accepted_airports: list[str] = []
    if slot_rows:
        accepted_airports = slot_rows[0].get("accepted_first_review_airports", [])
        if not accepted_airports:
            min_priority = min(r.get("priority_level", 999) for r in slot_rows)
            accepted_airports = sorted([r["airport_icao"] for r in slot_rows if r.get("priority_level") == min_priority])

    # Find the row corresponding to selected_airport
    selected_row = next((r for r in slot_rows if r.get("airport_icao") == selected_airport), None)

    expected_reason_code = ""
    expected_obs_time_utc = ""
    expected_obs_time_vn = ""

    if selected_row:
        expected_reason_code = derive_expected_reason_code(selected_row)
        expected_info = normalize_timestamp_pair(
            selected_row.get("observed_at_utc") or selected_row.get("review_slot_utc"),
            selected_row.get("raw_ob"),
        )
        expected_obs_time_utc = expected_info["utc_hhmm"]
        expected_obs_time_vn = expected_info["vn_hhmm"]

    # Check status
    if status != "COMPLETED":
        return {
            "is_passed": False,
            "failure_codes": ["ABANDONED"],
            "failure_message_vi": "Lượt thử bị bỏ dở hoặc chưa hoàn thành (ABANDONED).",
            "accepted_airports": accepted_airports,
            "expected_reason_code": expected_reason_code,
            "expected_obs_time_utc": expected_obs_time_utc,
            "expected_obs_time_vn": expected_obs_time_vn,
        }

    failure_codes: list[str] = []
    failure_messages: list[str] = []

    # 1. Airport check
    if selected_airport not in accepted_airports:
        failure_codes.append("WRONG_AIRPORT")
        failure_messages.append(
            f"Sân bay đã chọn ({selected_airport}) không thuộc danh sách ưu tiên cao nhất cần kiểm tra ({', '.join(accepted_airports)})"
        )

    # 2. Reason check
    if not selected_row:
        failure_codes.append("UNKNOWN_AIRPORT")
        failure_messages.append(f"Không tìm thấy dữ liệu cho sân bay {selected_airport}")
    else:
        matches_reason = (selected_reason_code == expected_reason_code)
        metar_present = selected_row.get("metar_is_present", True)
        metar_age = selected_row.get("metar_age_minutes")
        is_fresh = metar_present and (metar_age is None or metar_age <= 60.0)

        if not matches_reason and is_fresh and selected_row.get("has_precip_or_thunder_at_station") and (
            selected_row.get("is_low_visibility")
            or (selected_row.get("visibility_m") is not None and selected_row.get("visibility_m") < 5000)
        ):
            if selected_reason_code in ("PRECIP_OR_THUNDER_AT_STATION", "LOW_VISIBILITY"):
                matches_reason = True

        if not matches_reason:
            failure_codes.append("WRONG_REASON")
            expected_label = REASON_OPTIONS.get(expected_reason_code, expected_reason_code)
            failure_messages.append(
                f"Lý do đã chọn không chính xác (kỳ vọng: {expected_label})"
            )

        # 3. Observation time check
        if not matches_observation_time(entered_obs_time, selected_row):
            failure_codes.append("WRONG_OBS_TIME")
            failure_messages.append(
                f"Giờ bản tin căn cứ ({entered_obs_time}) không khớp với giờ quan trắc của trạm (kỳ vọng: {expected_obs_time_utc} / {expected_obs_time_vn})"
            )

    is_passed = len(failure_codes) == 0
    failure_message_vi = (
        "Chính xác! Lựa chọn sân bay, lý do và giờ quan trắc hoàn toàn chính xác."
        if is_passed
        else "; ".join(failure_messages)
    )

    return {
        "is_passed": is_passed,
        "failure_codes": failure_codes,
        "failure_message_vi": failure_message_vi,
        "accepted_airports": accepted_airports,
        "expected_reason_code": expected_reason_code,
        "expected_obs_time_utc": expected_obs_time_utc,
        "expected_obs_time_vn": expected_obs_time_vn,
    }


def append_kpi_trial(
    trial_record: dict[str, Any],
    log_path: str = "data/logs/kpi_trials_log.jsonl",
) -> None:
    """Append a KPI trial record to jsonl log file with timestamp and trial_id."""
    record = dict(trial_record)
    if "trial_id" not in record:
        record["trial_id"] = str(uuid.uuid4())
    if "recorded_at_utc" not in record:
        record["recorded_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    target_path = Path(log_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    with open(target_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_kpi_trials(
    log_path: str = "data/logs/kpi_trials_log.jsonl",
    include_seeds: bool = True,
) -> list[dict[str, Any]]:
    """Load KPI trials from jsonl log file (cached on the file signature; returns fresh dicts)."""
    path = Path(log_path)
    try:
        st = path.stat()
    except OSError:
        return []
    key = (str(path.resolve()), include_seeds)
    sig = (st.st_mtime_ns, st.st_size)
    hit = _TRIALS_CACHE.get(key)
    if hit is None or hit[0] != sig:
        hit = (sig, _read_kpi_trials(path, include_seeds))
        _TRIALS_CACHE[key] = hit
    return copy.deepcopy(hit[1])  # trials hold nested lists (failure_codes)


_TRIALS_CACHE: dict[tuple[str, bool], tuple[tuple[int, int], list[dict[str, Any]]]] = {}


def _read_kpi_trials(path: Path, include_seeds: bool) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            try:
                rec = json.loads(line_str)
                if not include_seeds and rec.get("is_sample_seed") is True:
                    continue
                records.append(rec)
            except Exception:
                continue
    return records


def compute_kpi_summary(trials: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute KPI summary statistics grouped by view_mode.
    
    Calculates total trials, passed trials, pass rate %, median duration
    of passed trials, time reduction %, and whether it meets the 30% reduction target.
    """
    grouped: dict[str, list[dict[str, Any]]] = {
        "baseline_3_sources": [],
        "dashboard": [],
    }

    for t in trials:
        mode = t.get("view_mode")
        if mode in grouped:
            grouped[mode].append(t)

    result: dict[str, Any] = {}

    for mode in ["baseline_3_sources", "dashboard"]:
        mode_trials = grouped[mode]
        total = len(mode_trials)
        passed = [t for t in mode_trials if t.get("is_passed") is True]
        abandoned = [t for t in mode_trials if t.get("status") != "COMPLETED"]
        passed_count = len(passed)
        pass_rate = round((passed_count / total * 100.0), 1) if total > 0 else 0.0

        passed_durations = [float(t["elapsed_seconds"]) for t in passed if t.get("elapsed_seconds") is not None]
        median_sec = float(statistics.median(passed_durations)) if passed_durations else None

        result[mode] = {
            "total_trials": total,
            "passed_trials": passed_count,
            "failed_trials": total - passed_count,
            "abandoned_trials": len(abandoned),
            "pass_rate_pct": pass_rate,
            "median_passed_seconds": median_sec,
        }

    base_median = result["baseline_3_sources"]["median_passed_seconds"]
    dash_median = result["dashboard"]["median_passed_seconds"]

    if base_median is not None and dash_median is not None and base_median > 0:
        time_reduction_pct = round(((base_median - dash_median) / base_median) * 100.0, 1)
        meets_target = time_reduction_pct >= 30.0
    else:
        time_reduction_pct = 0.0
        meets_target = False

    result["time_reduction_pct"] = time_reduction_pct
    result["meets_30_pct_target"] = meets_target

    return result

