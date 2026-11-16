"""METAR historical backfill from the AWC API, one airport per request, one window per request.

AWC API facts (verified against https://aviationweather.gov/data/schema/openapi.yaml and live probes):
- `date` (ISO 8601 or `yyyymmdd_hhmm`) is the END of the window, `hours` is how far back from it.
  Both ends are inclusive, so adjacent windows share one boundary observation (dedup happens downstream).
- At most 400 entries per request in total across all `ids`; the newest entries win, so multi-airport
  or multi-day requests silently truncate. Hence: one airport per request and 24 h windows (~49 entries).
- `date` older than 30 days answers HTTP 400; a window with no observations answers HTTP 204 (not an error).
- Rate limit: 100 requests/min; we stay far below it (>= 0.7 s between requests).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from src.ingestion.http_client import fetch_with_retry
from src.ingestion.metar_client import (
    METAR_API_BASE_URL,
    METAR_ATTRIBUTION,
    METAR_MAX_ENTRIES_PER_REQUEST,
    METAR_RETENTION_DAYS,
    parse_metar_payload,
)
from src.ingestion.raw_store import BatchConflictError, RawEnvelopeStore
from src.ingestion.storage import append_ingestion_log

logger = logging.getLogger(__name__)

MIN_REQUEST_INTERVAL_SEC = 0.7
DEFAULT_WINDOW_HOURS = 24
# METAR is half-hourly at VVDN/VVNB/VVTS; 4/h leaves 2x headroom for SPECI bursts.
ENTRIES_PER_HOUR_BOUND = 4
MAX_BACKFILL_DAYS = METAR_RETENTION_DAYS
_ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"
_COMPACT_FMT = "%Y%m%dT%H%M%SZ"


def max_window_hours(
    max_entries: int = METAR_MAX_ENTRIES_PER_REQUEST,
    entries_per_hour: int = ENTRIES_PER_HOUR_BOUND,
) -> int:
    """Largest window (hours) whose worst-case entry count (inclusive ends: hours*rate + 1) fits max_entries."""
    return (max_entries - 1) // entries_per_hour


def split_windows(
    start_utc: datetime,
    end_utc: datetime,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> List[Tuple[datetime, datetime]]:
    """Splits [start_utc, end_utc] into windows aligned to multiples of `window_hours` since the epoch.

    Alignment (midnight UTC for 24 h) makes every window_end independent of the run time, so reruns hit the
    same batch ids. The first window is the full aligned window containing `start_utc`; the last window may be
    shorter (ends at `end_utc`). Every window is at most `window_hours` long.
    """
    if window_hours < 1 or window_hours > max_window_hours():
        raise ValueError(
            f"window_hours must be in 1..{max_window_hours()} to stay under "
            f"{METAR_MAX_ENTRIES_PER_REQUEST} entries per request, got {window_hours}"
        )
    start_utc = _as_utc(start_utc)
    end_utc = _as_utc(end_utc)
    if end_utc <= start_utc:
        return []

    step = timedelta(hours=window_hours)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    window_start = epoch + ((start_utc - epoch) // step) * step

    windows: List[Tuple[datetime, datetime]] = []
    while window_start < end_utc:
        window_end = min(window_start + step, end_utc)
        windows.append((window_start, window_end))
        window_start += step
    return windows


def window_batch_id(airport: str, window_end_utc: datetime) -> str:
    """Deterministic batch id per (airport, window_end)."""
    return f"{_as_utc(window_end_utc).strftime(_COMPACT_FMT)}_metar_backfill_{airport.upper()}"


def _as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _ceil_hours(delta: timedelta) -> int:
    seconds = int(delta.total_seconds())
    return max(1, -(-seconds // 3600))


@dataclass
class _Throttle:
    """Guarantees a minimum spacing between request starts (injectable clock/sleeper for tests)."""

    min_interval_sec: float
    sleeper: Callable[[float], None]
    clock: Callable[[], float]
    _last_start: Optional[float] = None

    def wait(self) -> None:
        if self._last_start is not None:
            remaining = self.min_interval_sec - (self.clock() - self._last_start)
            if remaining > 0:
                self.sleeper(remaining)
        self._last_start = self.clock()


def backfill_metar(
    airports: Sequence[str],
    days: int,
    base_dir: str = "data",
    now_utc: Optional[datetime] = None,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    min_interval_sec: float = MIN_REQUEST_INTERVAL_SEC,
    fetcher: Callable[..., Any] = fetch_with_retry,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Dict[str, Any]:
    """Backfills `days` (1..30) of METAR/SPECI for each airport into the Bronze raw store.

    Each (airport, window) is one request and one envelope with `metadata.is_backfill = True`.
    Reruns are no-ops (deterministic batch id); a changed payload for an existing batch id is logged and skipped.
    Failures never abort the run. Every request is appended to <base_dir>/logs/ingestion_log.jsonl.
    """
    if not isinstance(days, int) or isinstance(days, bool) or not 1 <= days <= MAX_BACKFILL_DAYS:
        raise ValueError(f"days must be an integer in 1..{MAX_BACKFILL_DAYS}, got {days!r}")

    now = _as_utc(now_utc) if now_utc is not None else datetime.now(timezone.utc)
    range_end = now.replace(minute=0, second=0, microsecond=0)
    range_start = range_end - timedelta(days=days)
    windows = split_windows(range_start, range_end, window_hours)

    codes = [code.upper() for code in airports]
    store = RawEnvelopeStore(storage_root=base_dir)
    log_path = str(Path(base_dir) / "logs" / "ingestion_log.jsonl")
    throttle = _Throttle(min_interval_sec, sleeper, clock)

    summary: Dict[str, Any] = {
        "range_start_utc": range_start.strftime(_ISO_FMT),
        "range_end_utc": range_end.strftime(_ISO_FMT),
        "window_hours": window_hours,
        "airports": codes,
        "requests": 0,
        "inserted": 0,
        "noop": 0,
        "no_data": 0,
        "conflicts": 0,
        "failed": 0,
        "possibly_truncated": 0,
        "records_total": 0,
        "records_by_airport": {code: 0 for code in codes},
        "bytes_written": 0,
    }

    for code in codes:
        for window_start, window_end in windows:
            entry = _backfill_one_window(
                code, window_start, window_end, store, throttle, fetcher, log_path, summary
            )
            append_ingestion_log(entry, log_path=log_path)

    logger.info("METAR backfill finished: %s", summary)
    return summary


def _backfill_one_window(
    code: str,
    window_start: datetime,
    window_end: datetime,
    store: RawEnvelopeStore,
    throttle: _Throttle,
    fetcher: Callable[..., Any],
    log_path: str,
    summary: Dict[str, Any],
) -> Dict[str, Any]:
    """Fetches and stores one (airport, window); returns the ingestion-log entry. Never raises."""
    batch_id = window_batch_id(code, window_end)
    window_hours = _ceil_hours(window_end - window_start)
    start_iso = window_start.strftime(_ISO_FMT)
    end_iso = window_end.strftime(_ISO_FMT)
    fetched_iso = datetime.now(timezone.utc).strftime(_ISO_FMT)

    entry: Dict[str, Any] = {
        "batch_id": batch_id,
        "source": "metar",
        "airport": code,
        "expected_slot_utc": end_iso,
        "fetched_at_utc": fetched_iso,
        "status": "FAILED",
        "http_status": None,
        "record_count": 0,
        "bytes_written": 0,
        "file_path": None,
        "is_backfill": True,
        "backfill_window_start_utc": start_iso,
        "backfill_window_end_utc": end_iso,
        "inserted": False,
        "error_type": None,
        "error_message": None,
    }

    params = {"ids": code, "format": "json", "hours": str(window_hours), "date": end_iso}
    throttle.wait()
    summary["requests"] += 1
    started = time.monotonic()
    try:
        response = fetcher(METAR_API_BASE_URL, params=params)
        entry["http_status"] = response.status_code
        payload = parse_metar_payload(response)
    except Exception as err:  # noqa: BLE001 - one bad window must not abort the run
        entry.update(
            http_status=getattr(getattr(err, "response", None), "status_code", None),
            error_type=type(err).__name__,
            error_message=str(err),
        )
        summary["failed"] += 1
        return entry
    finally:
        entry["request_seconds"] = round(time.monotonic() - started, 3)

    record_count = len(payload)
    entry["record_count"] = record_count
    if record_count == 0:
        entry["status"] = "NO_DATA"
        summary["no_data"] += 1
        return entry

    truncated = record_count >= METAR_MAX_ENTRIES_PER_REQUEST
    if truncated:
        entry["possibly_truncated"] = True
        summary["possibly_truncated"] += 1
        logger.warning("METAR %s window ending %s returned %d entries (limit %d): possibly truncated",
                       code, end_iso, record_count, METAR_MAX_ENTRIES_PER_REQUEST)

    envelope = {
        "metadata": {
            "batch_id": batch_id,
            "source": "aviationweather_metar",
            "source_url": getattr(response, "url", None),
            "attribution": METAR_ATTRIBUTION,
            "expected_slot_utc": end_iso,
            "fetched_at_utc": fetched_iso,
            "http_status": response.status_code,
            "airports_requested": [code],
            "is_backfill": True,
            "backfill_window_start_utc": start_iso,
            "backfill_window_end_utc": end_iso,
            "record_count": record_count,
        },
        "payload": payload,
    }

    try:
        receipt = store.write(source="metar", date_str=window_start.strftime("%Y-%m-%d"), envelope=envelope)
    except BatchConflictError as err:
        entry.update(status="SKIPPED_CONFLICT", error_type="BatchConflictError", error_message=str(err))
        summary["conflicts"] += 1
        logger.warning("METAR backfill conflict skipped: %s", err)
        return entry
    except Exception as err:  # noqa: BLE001
        entry.update(error_type=type(err).__name__, error_message=str(err))
        summary["failed"] += 1
        return entry

    entry.update(
        status="SUCCESS",
        bytes_written=receipt.bytes_added,
        file_path=receipt.path,
        inserted=receipt.inserted,
        storage_layout=store.active_layout(),
    )
    summary["records_total"] += record_count
    summary["records_by_airport"][code] += record_count
    summary["bytes_written"] += receipt.bytes_added
    summary["inserted" if receipt.inserted else "noop"] += 1
    return entry
