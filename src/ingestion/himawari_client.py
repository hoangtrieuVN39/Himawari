import os
import re
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
import requests

from src.ingestion.http_client import fetch_with_retry
from src.ingestion.storage import append_ingestion_log

HIMAWARI9_S3_BASE_URL = "https://noaa-himawari9.s3.amazonaws.com"
DEFAULT_SEGMENTS = ["S0410", "S0510"]
DEFAULT_BAND = "B13"


def build_s3_segment_url(
    scan_dt_utc: datetime,
    segment: str,
    band: str = "B13"
) -> Tuple[str, str]:
    """
    Constructs the exact NOAA public AWS S3 URL and filename for a Himawari-9 B13 segment.
    Key format:
      AHI-L1b-FLDK/{YYYY}/{MM}/{DD}/{HHmm}/HS_H09_{YYYYMMDD}_{HHmm}_{band}_FLDK_R20_{segment}.DAT.bz2
    """
    if scan_dt_utc.tzinfo is None:
        scan_dt_utc = scan_dt_utc.replace(tzinfo=timezone.utc)
    else:
        scan_dt_utc = scan_dt_utc.astimezone(timezone.utc)

    yyyy = scan_dt_utc.strftime("%Y")
    mm = scan_dt_utc.strftime("%m")
    dd = scan_dt_utc.strftime("%d")
    hhmm = scan_dt_utc.strftime("%H%M")
    yyyymmdd = scan_dt_utc.strftime("%Y%m%d")

    filename = f"HS_H09_{yyyymmdd}_{hhmm}_{band}_FLDK_R20_{segment}.DAT.bz2"
    key = f"AHI-L1b-FLDK/{yyyy}/{mm}/{dd}/{hhmm}/{filename}"
    url = f"{HIMAWARI9_S3_BASE_URL}/{key}"
    return url, filename


def _extract_s3_xml_prefixes(xml_text: str) -> List[str]:
    """Extracts all <Prefix> values from S3 ListBucketResult XML."""
    return re.findall(r"<Prefix>([^<]+)</Prefix>", xml_text)


def _find_latest_slot_from_s3_xml(now_utc: datetime) -> Optional[datetime]:
    """
    Discovers the latest available Himawari-9 10-minute scan slot by querying
    S3 bucket XML listings. Handles cases where the local clock is in a future year
    or probes fail.
    """
    # 1. Try querying with the month prefix of now_utc
    month_prefix = f"AHI-L1b-FLDK/{now_utc.strftime('%Y/%m')}/"
    url = f"{HIMAWARI9_S3_BASE_URL}/?list-type=2&prefix={month_prefix}&delimiter=/"

    day_prefixes: List[str] = []
    try:
        resp = requests.get(url, timeout=10.0)
        if resp.status_code == 200:
            day_prefixes = [p for p in _extract_s3_xml_prefixes(resp.text) if re.search(r"\d{4}/\d{2}/\d{2}/", p)]
    except Exception:
        pass

    # 2. If month prefix returned no days, drill down from bucket root
    if not day_prefixes:
        root_url = f"{HIMAWARI9_S3_BASE_URL}/?list-type=2&prefix=AHI-L1b-FLDK/&delimiter=/"
        try:
            resp = requests.get(root_url, timeout=10.0)
            if resp.status_code == 200:
                years = sorted([p for p in _extract_s3_xml_prefixes(resp.text) if re.search(r"AHI-L1b-FLDK/\d{4}/", p)])
                if years:
                    latest_year = years[-1]
                    m_resp = requests.get(f"{HIMAWARI9_S3_BASE_URL}/?list-type=2&prefix={latest_year}&delimiter=/", timeout=10.0)
                    if m_resp.status_code == 200:
                        months = sorted([p for p in _extract_s3_xml_prefixes(m_resp.text) if re.search(r"AHI-L1b-FLDK/\d{4}/\d{2}/", p)])
                        if months:
                            latest_month = months[-1]
                            d_resp = requests.get(f"{HIMAWARI9_S3_BASE_URL}/?list-type=2&prefix={latest_month}&delimiter=/", timeout=10.0)
                            if d_resp.status_code == 200:
                                day_prefixes = [p for p in _extract_s3_xml_prefixes(d_resp.text) if re.search(r"\d{4}/\d{2}/\d{2}/", p)]
        except Exception:
            pass

    if not day_prefixes:
        return None

    # Pick the latest available day
    latest_day = sorted(day_prefixes)[-1]
    day_url = f"{HIMAWARI9_S3_BASE_URL}/?list-type=2&prefix={latest_day}&delimiter=/"

    try:
        resp = requests.get(day_url, timeout=10.0)
        if resp.status_code == 200:
            slot_prefixes = sorted([p for p in _extract_s3_xml_prefixes(resp.text) if re.search(r"\d{4}/\d{2}/\d{2}/\d{4}/", p)])
            if slot_prefixes:
                latest_slot_str = slot_prefixes[-1]
                match = re.search(r"(\d{4})/(\d{2})/(\d{2})/(\d{4})/", latest_slot_str)
                if match:
                    y, m, d, hm = match.groups()
                    hour = int(hm[:2])
                    minute = int(hm[2:])
                    return datetime(int(y), int(m), int(d), hour, minute, tzinfo=timezone.utc)
    except Exception:
        pass

    return None


def find_latest_available_slot(
    now_utc: Optional[datetime] = None,
    max_lookback_slots: int = 4
) -> datetime:
    """
    Finds the latest available 10-minute Himawari-9 slot on S3.
    First rounds now_utc - 15 minutes down to a 10-minute boundary, then probes
    up to max_lookback_slots via HTTP HEAD.
    If probing fails or clock is skewed ahead of real-world S3 archive, falls back
    to S3 bucket XML listing.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    elif now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    # Himawari-9 requires ~12-15 minutes after scan time to be available on S3
    target = now_utc - timedelta(minutes=15)
    rounded_minute = (target.minute // 10) * 10
    base_candidate = target.replace(minute=rounded_minute, second=0, microsecond=0)

    # Probe up to max_lookback_slots
    for i in range(max_lookback_slots):
        candidate_slot = base_candidate - timedelta(minutes=10 * i)
        url, _ = build_s3_segment_url(candidate_slot, segment="S0410", band="B13")
        try:
            resp = requests.head(url, timeout=5.0)
            if resp.status_code == 200:
                return candidate_slot
        except Exception:
            pass

    # Fallback to S3 XML listing
    fallback_slot = _find_latest_slot_from_s3_xml(now_utc)
    if fallback_slot is not None:
        return fallback_slot

    raise RuntimeError("No available Himawari-9 slot found on S3")


def download_himawari_slot(
    scan_dt_utc: datetime,
    segments: Optional[List[str]] = None,
    base_dir: str = "data",
    is_backfill: bool = False,
    source_dt_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Downloads requested Himawari-9 segments for the given scan slot.
    Saves atomically via .tmp file and os.replace into:
      <base_dir>/raw/himawari_hsd/date=YYYY-MM-DD/slot=HHMM/<filename>
    Logs audit record to <base_dir>/logs/ingestion_log.jsonl.
    If source_dt_utc is provided, builds S3 download URL from source_dt_utc while
    saving under scan_dt_utc timeline.
    """
    if segments is None:
        segments = list(DEFAULT_SEGMENTS)

    if scan_dt_utc.tzinfo is None:
        scan_dt_utc = scan_dt_utc.replace(tzinfo=timezone.utc)
    else:
        scan_dt_utc = scan_dt_utc.astimezone(timezone.utc)

    if source_dt_utc is not None:
        if source_dt_utc.tzinfo is None:
            source_dt_utc = source_dt_utc.replace(tzinfo=timezone.utc)
        else:
            source_dt_utc = source_dt_utc.astimezone(timezone.utc)

    now = datetime.now(timezone.utc)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_str = scan_dt_utc.strftime("%Y-%m-%d")
    slot_str = scan_dt_utc.strftime("%H%M")
    slot_iso = scan_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    timestamp_compact = scan_dt_utc.strftime("%Y%m%dT%H%M%SZ")

    random_suffix = uuid.uuid4().hex[:6]
    batch_id = f"{timestamp_compact}_himawari_{random_suffix}"

    target_slot_dir = Path(base_dir) / "raw" / "himawari_hsd" / f"date={date_str}" / f"slot={slot_str}"
    target_slot_dir.mkdir(parents=True, exist_ok=True)

    downloaded_files: Dict[str, str] = {}
    total_bytes = 0

    fetch_dt = source_dt_utc if source_dt_utc is not None else scan_dt_utc

    for seg in segments:
        url, _ = build_s3_segment_url(fetch_dt, segment=seg, band=DEFAULT_BAND)
        _, filename = build_s3_segment_url(scan_dt_utc, segment=seg, band=DEFAULT_BAND)
        target_path = target_slot_dir / filename
        temp_path = target_slot_dir / f"{filename}.tmp"

        try:
            resp = fetch_with_retry(url)
            content = resp.content

            with temp_path.open("wb") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())

            os.replace(temp_path, target_path)
            downloaded_files[seg] = str(target_path)
            total_bytes += len(content)
        except Exception:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

    expected_count = len(segments)
    downloaded_count = len(downloaded_files)

    if downloaded_count == expected_count:
        status = "SUCCESS"
    elif downloaded_count > 0:
        status = "PARTIAL"
    else:
        status = "FAILED"

    log_entry = {
        "batch_id": batch_id,
        "source": "himawari_hsd",
        "expected_slot_utc": slot_iso,
        "source_slot_utc": source_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if source_dt_utc else slot_iso,
        "fetched_at_utc": now_iso,
        "status": status,
        "expected_segments": expected_count,
        "downloaded_segments": downloaded_count,
        "bytes_written": total_bytes,
        "files": downloaded_files,
        "is_backfill": is_backfill,
        "error_type": None if status == "SUCCESS" else ("PartialDownload" if status == "PARTIAL" else "AllSegmentsFailed"),
        "error_message": None if status == "SUCCESS" else f"Downloaded {downloaded_count}/{expected_count} segments",
    }

    log_path = str(Path(base_dir) / "logs" / "ingestion_log.jsonl")
    append_ingestion_log(log_entry, log_path=log_path)

    return log_entry


def cleanup_old_raw_segments(
    base_dir: str = "data",
    retain_hours: int = 24,
    pinned_slots: Optional[List[str]] = None,
    now_utc: Optional[datetime] = None
) -> int:
    """
    Cleans up raw Himawari-9 .DAT.bz2 segment files according to retention policy.
    Only deletes a .DAT.bz2 file if:
      1. Its slot age is older than retain_hours hours, AND
      2. Its slot ISO string or compact format is NOT in pinned_slots, AND
      3. A corresponding metrics JSON file already exists in:
         <base_dir>/raw/himawari_metrics/date=YYYY-MM-DD/
         containing YYYYMMDD_HHMM in its filename.
    Returns the count of deleted files.
    """
    if pinned_slots is None:
        pinned_slots = []

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    elif now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    pinned_set = set()
    for s in pinned_slots:
        pinned_set.add(s)
        # Normalize: strip punctuation
        cleaned = re.sub(r"[^0-9a-zA-Z]", "", s)
        pinned_set.add(cleaned)

    raw_hsd_dir = Path(base_dir) / "raw" / "himawari_hsd"
    if not raw_hsd_dir.exists():
        return 0

    deleted_count = 0
    cutoff_delta = timedelta(hours=retain_hours)

    for file_path in raw_hsd_dir.rglob("*.DAT.bz2"):
        # Parse slot timestamp from filename or path
        # Example filename: HS_H09_20261101_0100_B13_FLDK_R20_S0410.DAT.bz2
        match = re.search(r"HS_H09_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})_", file_path.name)
        if match:
            y, m, d, hh, mm = match.groups()
            slot_dt = datetime(int(y), int(m), int(d), int(hh), int(mm), tzinfo=timezone.utc)
        else:
            # Fallback to date=YYYY-MM-DD and slot=HHMM in directory path
            d_match = re.search(r"date=(\d{4})-(\d{2})-(\d{2})", str(file_path))
            s_match = re.search(r"slot=(\d{2})(\d{2})", str(file_path))
            if d_match and s_match:
                y, m, d = d_match.groups()
                hh, mm = s_match.groups()
                slot_dt = datetime(int(y), int(m), int(d), int(hh), int(mm), tzinfo=timezone.utc)
            else:
                continue

        # Condition 1: Older than retain_hours
        slot_age = now_utc - slot_dt
        if slot_age < cutoff_delta:
            continue

        # Condition 2: Not pinned
        slot_iso = slot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        slot_compact = slot_dt.strftime("%Y%m%d_%H%M")
        slot_digits = slot_dt.strftime("%Y%m%d%H%M")

        is_pinned = (
            slot_iso in pinned_set or
            slot_compact in pinned_set or
            slot_digits in pinned_set or
            any(p in pinned_set for p in [slot_iso, slot_compact, slot_digits])
        )
        if is_pinned:
            continue

        # Condition 3: Corresponding metrics JSON file exists
        date_str = slot_dt.strftime("%Y-%m-%d")
        metrics_dir = Path(base_dir) / "raw" / "himawari_metrics" / f"date={date_str}"
        if not metrics_dir.exists():
            continue

        compact_key = slot_compact  # e.g. "20261101_0100"
        matching_metrics = [
            f for f in metrics_dir.glob("*.json")
            if compact_key in f.name
        ]
        if not matching_metrics:
            continue

        # All conditions met -> delete raw segment file
        try:
            file_path.unlink()
            deleted_count += 1
        except OSError:
            pass

    return deleted_count
