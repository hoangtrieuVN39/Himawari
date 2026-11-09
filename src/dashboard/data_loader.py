"""Data loading, timestamp normalization, and audit parsing for Streamlit Dashboard."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

from src.spark.transforms import parse_metar_raw_ob

VALID_AIRPORTS = ["VVDN", "VVNB", "VVTS"]


def _parse_utc_dt(ts_str: str | None) -> datetime | None:
    """Parse an ISO timestamp string to an aware UTC datetime."""
    if not ts_str:
        return None
    clean = str(ts_str).strip()
    try:
        if clean.endswith("Z"):
            return datetime.fromisoformat(clean.replace("Z", "+00:00")).astimezone(timezone.utc)
        if "+" in clean or clean.count("-") > 2:
            return datetime.fromisoformat(clean).astimezone(timezone.utc)
        naive = datetime.fromisoformat(clean)
        if naive.hour >= 13:
            return (naive - timedelta(hours=7)).replace(tzinfo=timezone.utc)
        return naive.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _round_up_5min_utc(dt: datetime) -> datetime:
    """Round a UTC datetime up to the next 5-minute boundary."""
    rem = dt.minute % 5
    if rem != 0 or dt.second > 0 or dt.microsecond > 0:
        return dt.replace(second=0, microsecond=0) + timedelta(minutes=(5 - rem))
    return dt.replace(second=0, microsecond=0)


def _classify_b13_cloud(rec: dict[str, Any]) -> tuple[str, str]:
    """Classify B13 cloud tag and Vietnamese summary identically to enrich_valid_himawari."""
    inner_sev = float(rec.get("inner_severe_convection_fraction") or 0.0)
    inner_cold = float(rec.get("inner_cold_cloud_fraction") or 0.0)
    outer_cold = float(rec.get("outer_cold_cloud_fraction") or 0.0)
    inner_bt_k = rec.get("inner_bt_min_k")
    bt_k = float(inner_bt_k) if inner_bt_k is not None else 999.0

    if inner_sev >= 0.05 or bt_k < 220.0:
        return (
            "SEVERE_CONVECTION_AT_STATION",
            "Đỉnh mây dông phát triển rất mạnh (< -53.15°C) ngay trên sân bay",
        )
    if inner_cold >= 0.05 or bt_k < 235.0:
        return (
            "COLD_CONVECTIVE_CLOUD_AT_STATION",
            "Có mây đối lưu lạnh (< -38.15°C) tại trạm",
        )
    if outer_cold >= 0.05:
        return (
            "COLD_CLOUD_IN_OUTER_VICINITY",
            "Có dải mây đối lưu lạnh trong vùng lân cận ±1.0°",
        )
    return ("WARM_OR_CLEAR_CLOUD", "Mây tầng thấp hoặc trời ít mây")


def _compute_slot_rows_from_raw(storage_root: str, slot_utc_iso: str) -> list[dict[str, Any]]:
    """Build the 3 airport decision rows for slot_utc_iso from raw envelopes (identical to PySpark rules)."""
    root = Path(storage_root)
    slot_dt = _parse_utc_dt(slot_utc_iso)
    if slot_dt is None:
        return []
    slot_str = slot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    target_fc_dt = slot_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    target_fc_str = target_fc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Scan valid METAR records <= slot_dt
    metar_candidates: dict[str, list[dict[str, Any]]] = {ap: [] for ap in VALID_AIRPORTS}
    for m_file in sorted((root / "raw" / "metar").glob("date=*/*.json")):
        try:
            env = json.loads(m_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        meta = env.get("metadata") or {}
        fetched_dt = _parse_utc_dt(meta.get("fetched_at_utc"))
        is_backfill = bool(meta.get("is_backfill", False))
        if fetched_dt and fetched_dt > slot_dt and not is_backfill:
            continue
        for item in env.get("payload") or []:
            icao = item.get("icaoId")
            if icao not in metar_candidates:
                continue
            raw_ob = item.get("rawOb") or ""
            if not raw_ob or "NIL" in raw_ob.split():
                continue
            obs_epoch = item.get("obsTime")
            if obs_epoch is not None:
                obs_dt = datetime.fromtimestamp(int(obs_epoch), tz=timezone.utc)
            else:
                obs_dt = _parse_utc_dt(item.get("reportTime"))
            if obs_dt is None or obs_dt > slot_dt:
                continue
            receipt_dt = _parse_utc_dt(item.get("receiptTime"))
            parsed_wx = parse_metar_raw_ob(raw_ob, str(item.get("visib")) if item.get("visib") is not None else None)
            metar_candidates[icao].append({
                "obs_dt": obs_dt,
                "fetched_dt": fetched_dt or obs_dt,
                "receipt_dt": receipt_dt or fetched_dt or obs_dt,
                "is_amended_or_speci": parsed_wx["is_amended_or_speci"],
                "raw_ob": raw_ob,
                "parsed": parsed_wx,
            })

    # 2. Scan valid Open-Meteo records for target_fc_dt with fetched_at <= slot_dt
    fc_candidates: dict[str, list[dict[str, Any]]] = {ap: [] for ap in VALID_AIRPORTS}
    for f_file in sorted((root / "raw" / "open_meteo").glob("date=*/*.json")):
        try:
            env = json.loads(f_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        meta = env.get("metadata") or {}
        icao = meta.get("airport_icao")
        if icao not in fc_candidates:
            continue
        fetched_dt = _parse_utc_dt(meta.get("fetched_at_utc"))
        is_backfill = bool(meta.get("is_backfill", False))
        if fetched_dt and fetched_dt > slot_dt and not is_backfill:
            continue
        hourly = (env.get("payload") or {}).get("hourly") or {}
        times = hourly.get("time") or []
        precip = hourly.get("precipitation") or []
        rain = hourly.get("rain") or []
        showers = hourly.get("showers") or []
        wcode = hourly.get("weather_code") or []
        vis = hourly.get("visibility") or []
        for idx, t_str in enumerate(times):
            t_dt = _parse_utc_dt(t_str + ":00Z" if len(str(t_str)) == 16 else t_str)
            if t_dt == target_fc_dt:
                p_val = float(precip[idx]) if idx < len(precip) and precip[idx] is not None else 0.0
                r_val = float(rain[idx]) if idx < len(rain) and rain[idx] is not None else 0.0
                s_val = float(showers[idx]) if idx < len(showers) and showers[idx] is not None else 0.0
                if p_val < 0 or r_val < 0 or s_val < 0:
                    continue
                fc_candidates[icao].append({
                    "fetched_dt": fetched_dt or target_fc_dt,
                    "precipitation_mm": p_val,
                    "rain_mm": r_val,
                    "showers_mm": s_val,
                    "forecast_weather_code": int(wcode[idx]) if idx < len(wcode) and wcode[idx] is not None else None,
                    "forecast_visibility_m": float(vis[idx]) if idx < len(vis) and vis[idx] is not None else None,
                    "forecast_rain_next_hour_mm": max(p_val, r_val + s_val),
                })

    # 3. Scan valid Himawari B13 metrics <= slot_dt
    hima_candidates: dict[str, list[dict[str, Any]]] = {ap: [] for ap in VALID_AIRPORTS}
    for h_file in sorted((root / "raw" / "himawari_metrics").glob("date=*/*.json")):
        try:
            env = json.loads(h_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        meta = env.get("metadata") or {}
        if meta.get("is_synthetic_test"):
            continue
        for ap_item in env.get("airports") or []:
            icao = ap_item.get("airport_icao")
            if icao not in hima_candidates or ap_item.get("status") != "AVAILABLE":
                continue
            scan_dt = _parse_utc_dt(ap_item.get("scan_time_utc"))
            fetched_dt = _parse_utc_dt(ap_item.get("fetched_at_utc") or meta.get("processed_at_utc"))
            if scan_dt is None or scan_dt > slot_dt:
                continue
            vf = ap_item.get("b13_valid_fraction")
            bt_k = ap_item.get("inner_bt_min_k")
            if vf is None or not (0.0 <= float(vf) <= 1.0):
                continue
            if bt_k is None or not (150.0 <= float(bt_k) <= 350.0):
                continue
            tag, summary_vi = _classify_b13_cloud(ap_item)
            hima_candidates[icao].append({
                "scan_dt": scan_dt,
                "fetched_dt": fetched_dt or scan_dt,
                "png_rel_path": ap_item.get("png_rel_path"),
                "b13_valid_fraction": float(vf),
                "inner_bt_min_k": float(bt_k),
                "inner_bt_min_c": float(ap_item.get("inner_bt_min_c")),
                "inner_cold_cloud_fraction": float(ap_item.get("inner_cold_cloud_fraction") or 0.0),
                "inner_severe_convection_fraction": float(ap_item.get("inner_severe_convection_fraction") or 0.0),
                "b13_cloud_tag": tag,
                "b13_summary_vi": summary_vi,
            })

    # 4. Assemble 3 airport rows and apply Level 1-4 priority rules
    rows: list[dict[str, Any]] = []
    for icao in VALID_AIRPORTS:
        m_list = metar_candidates[icao]
        m_best = None
        rev_idx = 0
        if m_list:
            # Deduplicate revisions per obs_dt first, then pick latest obs_dt
            m_list.sort(key=lambda x: (x["obs_dt"], x["is_amended_or_speci"], x["receipt_dt"], x["fetched_dt"]))
            latest_obs = m_list[-1]["obs_dt"]
            same_obs = [x for x in m_list if x["obs_dt"] == latest_obs]
            # Count distinct raw_ob revisions for this observation timestamp
            seen_raw: list[str] = []
            for x in same_obs:
                if x["raw_ob"] not in seen_raw:
                    seen_raw.append(x["raw_ob"])
            rev_idx = len(seen_raw)
            m_best = same_obs[-1]

        f_list = fc_candidates[icao]
        f_best = None
        if f_list:
            f_list.sort(key=lambda x: x["fetched_dt"])
            f_best = f_list[-1]

        h_list = hima_candidates[icao]
        h_best = None
        if h_list:
            h_list.sort(key=lambda x: (x["scan_dt"], x["fetched_dt"]))
            h_best = h_list[-1]

        metar_present = m_best is not None
        obs_utc_str = m_best["obs_dt"].strftime("%Y-%m-%dT%H:%M:%SZ") if m_best else None
        obs_hhmm = m_best["obs_dt"].strftime("%H:%MZ") if m_best else ""
        metar_fetched_str = m_best["fetched_dt"].strftime("%Y-%m-%dT%H:%M:%SZ") if m_best else None
        metar_age = round((slot_dt - m_best["obs_dt"]).total_seconds() / 60.0, 1) if m_best else None
        parsed = m_best["parsed"] if m_best else parse_metar_raw_ob(None)

        fc_present = f_best is not None
        fc_rain = f_best["forecast_rain_next_hour_mm"] if f_best else None

        # Priority Level 1-4 evaluation
        is_l1 = (
            metar_present
            and metar_age is not None
            and metar_age <= 60.0
            and (parsed["has_precip_or_thunder_at_station"] or parsed["is_low_visibility"])
        )
        is_l2 = (not is_l1) and ((not metar_present) or (metar_age is not None and metar_age > 60.0))
        is_l3 = (not is_l1) and (not is_l2) and fc_present and (fc_rain is not None and fc_rain >= 0.1)

        if is_l1:
            p_level = 1
            p_label = "LEVEL_1_HAZARD_AT_STATION"
            wx_str = ", ".join(parsed["station_weather_codes"])
            vis_m = parsed["visibility_m"]
            if parsed["has_precip_or_thunder_at_station"] and parsed["is_low_visibility"]:
                reason_vi = f"METAR mới ({obs_hhmm}) ghi nhận mưa/dông tại trạm ({wx_str}) và tầm nhìn thấp {vis_m}m (< 5km)"
            elif parsed["has_precip_or_thunder_at_station"]:
                vis_txt = str(vis_m) if vis_m is not None else "không rõ"
                reason_vi = f"METAR mới ({obs_hhmm}) ghi nhận mưa/dông tại trạm ({wx_str}), tầm nhìn {vis_txt}m"
            else:
                reason_vi = f"METAR mới ghi nhận tầm nhìn thấp {vis_m}m (< 5km)"
        elif is_l2:
            p_level = 2
            p_label = "LEVEL_2_DATA_VERIFICATION"
            if not metar_present:
                reason_vi = "Cần xác minh dữ liệu: Thiếu bản tin quan trắc METAR"
            else:
                reason_vi = f"Cần xác minh dữ liệu: Bản tin METAR đã cũ {metar_age} phút (> 60 phút)"
        elif is_l3:
            p_level = 3
            p_label = "LEVEL_3_FORECAST_RAIN_ALERT"
            reason_vi = (
                f"Điều kiện tại trạm tốt, nhưng dự báo mưa giờ tới đạt "
                f"{round(float(fc_rain), 1)} mm (hiệu lực {target_fc_dt.strftime('%H:%MZ')})"
            )
        else:
            p_level = 4
            p_label = "LEVEL_4_ROUTINE"
            reason_vi = "Điều kiện tại trạm bình thường, không có cảnh báo mưa giờ tới"

        rows.append({
            "review_slot_utc": slot_str,
            "airport_icao": icao,
            "observed_at_utc": obs_utc_str,
            "metar_fetched_at_utc": metar_fetched_str,
            "raw_ob": m_best["raw_ob"] if m_best else None,
            "visibility_m": parsed["visibility_m"],
            "visibility_capped_9999": parsed["visibility_capped_9999"],
            "is_low_visibility": parsed["is_low_visibility"],
            "has_precip_or_thunder_at_station": parsed["has_precip_or_thunder_at_station"],
            "station_weather_codes": parsed["station_weather_codes"],
            "has_vicinity_weather": parsed["has_vicinity_weather"],
            "vicinity_weather_codes": parsed["vicinity_weather_codes"],
            "trend_weather_codes": parsed["trend_weather_codes"],
            "is_amended_or_speci": parsed["is_amended_or_speci"],
            "revision_index": rev_idx if metar_present else None,
            "metar_is_present": metar_present,
            "metar_age_minutes": metar_age,
            "target_forecast_hour_utc": target_fc_str if fc_present else None,
            "forecast_fetched_at_utc": f_best["fetched_dt"].strftime("%Y-%m-%dT%H:%M:%SZ") if f_best else None,
            "precipitation_mm": f_best["precipitation_mm"] if f_best else None,
            "rain_mm": f_best["rain_mm"] if f_best else None,
            "showers_mm": f_best["showers_mm"] if f_best else None,
            "forecast_weather_code": f_best["forecast_weather_code"] if f_best else None,
            "forecast_visibility_m": f_best["forecast_visibility_m"] if f_best else None,
            "forecast_is_present": fc_present if fc_present else None,
            "forecast_rain_next_hour_mm": fc_rain,
            "latest_b13_scan_time_utc": h_best["scan_dt"].strftime("%Y-%m-%dT%H:%M:%SZ") if h_best else None,
            "latest_b13_png_path": h_best["png_rel_path"] if h_best else None,
            "b13_valid_fraction": h_best["b13_valid_fraction"] if h_best else None,
            "inner_bt_min_k": h_best["inner_bt_min_k"] if h_best else None,
            "inner_bt_min_c": h_best["inner_bt_min_c"] if h_best else None,
            "inner_cold_cloud_fraction": h_best["inner_cold_cloud_fraction"] if h_best else None,
            "inner_severe_convection_fraction": h_best["inner_severe_convection_fraction"] if h_best else None,
            "b13_cloud_tag": h_best["b13_cloud_tag"] if h_best else None,
            "b13_summary_vi": h_best["b13_summary_vi"] if h_best else None,
            "b13_is_present": h_best is not None,
            "priority_level": p_level,
            "priority_label": p_label,
            "reason_vi": reason_vi,
        })

    # Co-priority top resolution across the 3 airports
    min_p = min(r["priority_level"] for r in rows)
    accepted = sorted(r["airport_icao"] for r in rows if r["priority_level"] == min_p)
    for r in rows:
        r["top_priority_level"] = min_p
        r["is_top_priority"] = r["priority_level"] == min_p
        r["accepted_first_review_airports"] = accepted
        r["co_priority_top_count"] = len(accepted)
        r["is_co_priority_top"] = r["is_top_priority"] and len(accepted) > 1

    return rows


def sync_latest_raw_to_decision_table(
    storage_root: str = "data",
    force_latest: bool = False,
) -> list[str]:
    """Ensure all live non-backfill raw METAR batches in storage_root are reflected in decision_table.jsonl."""
    root = Path(storage_root)
    metar_dir = root / "raw" / "metar"
    dec_path = root / "processed" / "decision_table" / "decision_table.jsonl"
    if not metar_dir.exists() or not dec_path.exists():
        return []

    # Discover all live non-backfill slots from raw METAR envelopes
    discovered_slots: set[str] = set()
    for m_file in sorted(metar_dir.glob("date=*/*.json")):
        try:
            meta = (json.loads(m_file.read_text(encoding="utf-8"))).get("metadata") or {}
            if meta.get("is_backfill"):
                continue
            fetched_dt = _parse_utc_dt(meta.get("fetched_at_utc"))
            if fetched_dt:
                slot_dt = _round_up_5min_utc(fetched_dt)
                discovered_slots.add(slot_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
        except Exception:
            continue

    if not discovered_slots:
        return []

    # Load existing rows keyed by (normalized_slot_utc, airport_icao)
    existing_by_slot: dict[str, list[dict[str, Any]]] = {}
    with open(dec_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                row = json.loads(line.strip())
                norm_slot = normalize_timestamp_pair(row.get("review_slot_utc"))["utc_iso"]
                row["review_slot_utc"] = norm_slot
                existing_by_slot.setdefault(norm_slot, []).append(row)
            except Exception:
                continue

    latest_discovered = max(discovered_slots)
    slots_to_compute = [
        s for s in sorted(discovered_slots)
        if s not in existing_by_slot or (force_latest and s == latest_discovered)
    ]
    if not slots_to_compute:
        return []

    for slot_iso in slots_to_compute:
        new_rows = _compute_slot_rows_from_raw(storage_root, slot_iso)
        if len(new_rows) == len(VALID_AIRPORTS):
            existing_by_slot[slot_iso] = new_rows

    # Write back all slots in chronological order
    with open(dec_path, "w", encoding="utf-8") as f:
        for slot_iso in sorted(existing_by_slot.keys()):
            for r in existing_by_slot[slot_iso]:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return slots_to_compute


def fetch_and_update_realtime(
    storage_root: str = "data",
    include_himawari: bool = True,
) -> dict[str, Any]:
    """Trigger live ingestion from AviationWeather, Open-Meteo, and Himawari-9 S3, then sync decision_table.jsonl."""
    cmd = [
        sys.executable,
        "-m",
        "src.ingestion.run_ingest",
        "--once",
        "--storage-root",
        storage_root,
    ]
    if include_himawari:
        cmd.append("--include-himawari")

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if proc.returncode != 0:
        return {
            "status": "ERROR",
            "error": (proc.stderr or proc.stdout or "Unknown ingestion error")[:500],
        }

    synced_slots = sync_latest_raw_to_decision_table(storage_root=storage_root, force_latest=True)
    slots = load_decision_slots(storage_root)
    latest_slot = max(slots.keys()) if slots else None
    return {
        "status": "SUCCESS",
        "synced_slots": synced_slots,
        "latest_slot_utc": latest_slot,
    }


def normalize_timestamp_pair(ts_str: str | None, raw_ob: str | None = None) -> dict[str, str]:
    """Normalize timestamp to both UTC and Vietnam time (ICT, UTC+7).
    
    Cross-checks the true UTC DDHHMMZ token in raw_ob (e.g. 090700Z) to correctly
    recover UTC hours/minutes even when Windows PySpark serialized UTC timestamps
    as local naive ISO strings (e.g. 14:00:00).
    
    Returns:
        dict with keys:
            - utc_iso: e.g. "2026-11-09T07:00:00Z"
            - utc_hhmm: e.g. "07:00Z"
            - vn_iso: e.g. "2026-11-09 14:00:00 ICT (UTC+7)"
            - vn_hhmm: e.g. "14:00"
            - metar_ddhhmmz: e.g. "090700Z"
    """
    utc_dt: datetime | None = None
    metar_ddhhmmz = ""

    # 1. Attempt extracting canonical METAR UTC observation token (DDHHMMZ)
    if raw_ob:
        match = re.search(r"\b(\d{2})(\d{2})(\d{2})Z\b", raw_ob)
        if match:
            day = int(match.group(1))
            hh = int(match.group(2))
            mm = int(match.group(3))
            metar_ddhhmmz = match.group(0)

            year = 2026
            month = 11
            if ts_str:
                d_match = re.match(r"^(\d{4})-(\d{2})-(\d{2})", ts_str)
                if d_match:
                    year = int(d_match.group(1))
                    month = int(d_match.group(2))

            utc_dt = datetime(year, month, day, hh, mm, 0, tzinfo=timezone.utc)

    # 2. If no valid METAR token, parse ts_str directly
    if utc_dt is None and ts_str:
        clean_ts = ts_str.strip()
        if clean_ts.endswith("Z"):
            utc_dt = datetime.fromisoformat(clean_ts.replace("Z", "+00:00"))
        elif "+" in clean_ts or clean_ts.count("-") > 2:
            parsed = datetime.fromisoformat(clean_ts)
            utc_dt = parsed.astimezone(timezone.utc)
        else:
            # Naive ISO string (e.g. "2026-11-09T14:00:00")
            naive = datetime.fromisoformat(clean_ts)
            # Heuristic for Windows PySpark collect() UTC+7 offset
            if naive.hour >= 13:
                utc_dt = (naive - timedelta(hours=7)).replace(tzinfo=timezone.utc)
            else:
                utc_dt = naive.replace(tzinfo=timezone.utc)
        metar_ddhhmmz = f"{utc_dt.day:02d}{utc_dt.hour:02d}{utc_dt.minute:02d}Z"

    # 3. Fallback default if neither raw_ob nor ts_str gave a valid datetime
    if utc_dt is None:
        utc_dt = datetime(2026, 11, 9, 7, 0, 0, tzinfo=timezone.utc)
        metar_ddhhmmz = "090700Z"

    vn_dt = utc_dt + timedelta(hours=7)

    return {
        "utc_iso": utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "utc_hhmm": utc_dt.strftime("%H:%MZ"),
        "vn_iso": f"{vn_dt.strftime('%Y-%m-%d %H:%M:%S')} ICT (UTC+7)",
        "vn_hhmm": vn_dt.strftime("%H:%M"),
        "metar_ddhhmmz": metar_ddhhmmz,
    }


def load_decision_slots(storage_root: str = "data") -> dict[str, list[dict[str, Any]]]:
    """Load decision table from decision_table.jsonl and group by normalized UTC slot.
    
    Automatically syncs any newer non-backfill raw METAR/Open-Meteo/Himawari batches on disk
    into decision_table.jsonl before loading.
    
    Returns:
        dict[utc_iso, list[row_dict]] where utc_iso is e.g. "2026-11-09T07:00:00Z"
    """
    sync_latest_raw_to_decision_table(storage_root)
    path = Path(storage_root) / "processed" / "decision_table" / "decision_table.jsonl"
    if not path.exists():
        return {}

    slots: dict[str, list[dict[str, Any]]] = {}

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            try:
                row = json.loads(line_str)
            except Exception:
                continue

            slot_info = normalize_timestamp_pair(row.get("review_slot_utc"))
            obs_info = normalize_timestamp_pair(row.get("observed_at_utc"), row.get("raw_ob"))

            row["slot_utc_iso"] = slot_info["utc_iso"]
            row["slot_utc_hhmm"] = slot_info["utc_hhmm"]
            row["slot_vn_iso"] = slot_info["vn_iso"]
            row["slot_vn_hhmm"] = slot_info["vn_hhmm"]
            row["slot_display_label"] = f"{slot_info['utc_hhmm']} ({slot_info['vn_hhmm']} ICT)"

            row["obs_utc_iso"] = obs_info["utc_iso"]
            row["obs_utc_hhmm"] = obs_info["utc_hhmm"]
            row["obs_vn_iso"] = obs_info["vn_iso"]
            row["obs_vn_hhmm"] = obs_info["vn_hhmm"]
            row["obs_ddhhmmz"] = obs_info["metar_ddhhmmz"]

            key = slot_info["utc_iso"]
            if key not in slots:
                slots[key] = []
            slots[key].append(row)

    # Ensure accepted_first_review_airports is present and consistent on all rows in each slot
    for slot_key, rows in slots.items():
        if rows:
            # If not present in raw row, compute from minimum priority_level
            min_priority = min(r.get("priority_level", 999) for r in rows)
            default_accepted = sorted([r["airport_icao"] for r in rows if r.get("priority_level") == min_priority])
            for r in rows:
                if not r.get("accepted_first_review_airports"):
                    r["accepted_first_review_airports"] = default_accepted

    return slots


def sort_slot_rows(rows: list[dict[str, Any]], sort_mode: str = "priority") -> list[dict[str, Any]]:
    """Sort a list of airport rows for a given slot.
    
    Supported sort_mode values:
        - "priority": priority_level ASC, airport_icao ASC (default)
        - "icao_asc": airport_icao ASC
        - "icao_desc": airport_icao DESC (reverses co-priority rows to test tie invariance)
    """
    copied = [dict(r) for r in rows]
    if sort_mode == "priority":
        copied.sort(key=lambda r: (r.get("priority_level", 999), r.get("airport_icao", "")))
    elif sort_mode == "icao_asc":
        copied.sort(key=lambda r: r.get("airport_icao", ""))
    elif sort_mode == "icao_desc":
        copied.sort(key=lambda r: r.get("airport_icao", ""), reverse=True)
    return copied


def get_b13_image_sequence(
    storage_root: str,
    airport_icao: str,
    up_to_utc_iso: str,
    max_frames: int = 6,
) -> list[dict[str, str]]:
    """Scan and return up to max_frames B13 PNG images chronologically <= up_to_utc_iso.
    
    Prevents future data leakage by strictly checking scan_time <= up_to_utc_iso.
    
    Returns:
        list of dict: [{"scan_time_utc": str, "utc_hhmm": str, "vn_hhmm": str, "png_path": str}]
    """
    airport_dir = Path(storage_root) / "processed" / "himawari_png" / f"airport={airport_icao}"
    if not airport_dir.exists():
        return []

    norm_target = normalize_timestamp_pair(up_to_utc_iso)
    cutoff_dt = datetime.fromisoformat(norm_target["utc_iso"].replace("Z", "+00:00"))

    frames: list[dict[str, Any]] = []
    pattern = re.compile(r"B13_[A-Z0-9]+_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})Z\.png", re.IGNORECASE)

    for png_file in airport_dir.rglob("*.png"):
        match = pattern.search(png_file.name)
        if not match:
            continue

        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        hh = int(match.group(4))
        mm = int(match.group(5))

        dt_scan = datetime(year, month, day, hh, mm, 0, tzinfo=timezone.utc)
        if dt_scan <= cutoff_dt:
            vn_dt = dt_scan + timedelta(hours=7)
            frames.append({
                "_dt": dt_scan,
                "scan_time_utc": dt_scan.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "utc_hhmm": f"{hh:02d}:{mm:02d}Z",
                "vn_hhmm": f"{vn_dt.hour:02d}:{vn_dt.minute:02d}",
                "png_path": str(png_file).replace("\\", "/"),
            })

    frames.sort(key=lambda x: x["_dt"])
    recent_frames = frames[-max_frames:]

    for f in recent_frames:
        f.pop("_dt", None)

    return recent_frames


def load_audit_and_reconciliation(storage_root: str = "data") -> dict[str, Any]:
    """Load reconciliation summary, quarantine records, synthetic quarantine, and operational logs."""
    root = Path(storage_root)

    # 1. Reconciliation summary
    recon_path = root / "processed" / "reconciliation" / "reconciliation_summary.json"
    reconciliation = {"all_sources_reconciled": False, "sources": []}
    if recon_path.exists():
        try:
            with open(recon_path, "r", encoding="utf-8") as f:
                reconciliation = json.load(f)
        except Exception:
            pass

    # 2. Quarantine records
    quarantine_path = root / "processed" / "quarantine" / "quarantine_records.jsonl"
    quarantine_records: list[dict[str, Any]] = []
    if quarantine_path.exists():
        with open(quarantine_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        quarantine_records.append(json.loads(line_str))
                    except Exception:
                        pass

    # 3. Synthetic quarantine preview
    synthetic_preview: list[dict[str, Any]] = []
    syn_dir = root / "raw" / "synthetic_quarantine"
    if syn_dir.exists():
        for f in sorted(syn_dir.glob("*.json")):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                    meta = data.get("metadata", {})
                    if "airports" in data:
                        for ap in data["airports"]:
                            synthetic_preview.append({
                                "source": meta.get("source", "himawari_metrics"),
                                "batch_id": meta.get("batch_id"),
                                "file": f.name,
                                "airport_icao": ap.get("airport_icao"),
                                "status": ap.get("status"),
                                "error_hint": "B13 fraction > 1.0 or BT out of range",
                                "payload": ap,
                            })
                    elif "payload" in data:
                        for item in data["payload"]:
                            synthetic_preview.append({
                                "source": meta.get("source", "aviationweather_metar"),
                                "batch_id": meta.get("batch_id"),
                                "file": f.name,
                                "airport_icao": item.get("icaoId"),
                                "error_hint": "Unmonitored ICAO (VVXX), Future timestamp, or NIL METAR",
                                "payload": item,
                            })
                    else:
                        synthetic_preview.append(data)
            except Exception:
                pass

    # 4. Binary file errors (Himawari corrupted files)
    file_errors_path = root / "logs" / "himawari_file_errors.jsonl"
    binary_file_errors: list[dict[str, Any]] = []
    if file_errors_path.exists():
        with open(file_errors_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        binary_file_errors.append(json.loads(line_str))
                    except Exception:
                        pass

    # 5. Ingestion summary and logs
    ingest_log_path = root / "logs" / "ingestion_log.jsonl"
    ingestion_logs: list[dict[str, Any]] = []
    total_bytes = 0
    total_records = 0
    by_source: dict[str, int] = {}

    if ingest_log_path.exists():
        with open(ingest_log_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        entry = json.loads(line_str)
                        ingestion_logs.append(entry)
                        total_bytes += int(entry.get("bytes_written") or 0)
                        total_records += int(entry.get("record_count") or 0)
                        src = entry.get("source", "unknown")
                        by_source[src] = by_source.get(src, 0) + 1
                    except Exception:
                        pass

    ingestion_summary = {
        "total_logs": len(ingestion_logs),
        "total_bytes": total_bytes,
        "total_records": total_records,
        "by_source": by_source,
        "recent_logs": ingestion_logs[-10:] if ingestion_logs else [],
    }

    # 6. HDFS sync logs
    hdfs_log_path = root / "logs" / "hdfs_sync_log.jsonl"
    hdfs_logs: list[dict[str, Any]] = []
    if hdfs_log_path.exists():
        with open(hdfs_log_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        hdfs_logs.append(json.loads(line_str))
                    except Exception:
                        pass

    hdfs_summary = {
        "total_syncs": len(hdfs_logs),
        "logs": hdfs_logs,
    }

    return {
        "reconciliation": reconciliation,
        "quarantine_records": quarantine_records,
        "synthetic_quarantine_preview": synthetic_preview,
        "binary_file_errors": binary_file_errors,
        "ingestion_summary": ingestion_summary,
        "raw_ingestion_logs": ingestion_logs,
        "hdfs_summary": hdfs_summary,
    }
