"""Per-source data profile from Bronze envelopes (``RawEnvelopeStore.iter_envelopes``).

For each source the flattened records are profiled: row count, missing rate of key fields, exact-duplicate
rate (identical record content, per-fetch volatile fields excluded) and min/p50/max of numeric fields.
METAR is re-fetched with a 2 h look-back every 10 min, so a high duplicate rate there is expected and is
what the Spark dedupe step removes.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta
from typing import Any, Iterator, Optional

import numpy as np

from src.diagnostics.health import iso_z, iter_bronze

VOLATILE_FIELDS = frozenset({"fetched_at_utc", "processed_at_utc", "receiptTime"})

KEY_FIELDS: dict[str, tuple[str, ...]] = {
    "metar": ("icaoId", "obsTime", "reportTime", "rawOb", "temp", "dewp", "wdir", "wspd", "visib", "altim"),
    "open_meteo": (
        "airport_icao", "time", "precipitation", "rain", "showers", "weather_code", "visibility",
        "wind_speed_10m", "wind_gusts_10m",
    ),
    "himawari_metrics": (
        "airport_icao", "segment_code", "scan_time_utc", "status", "b13_valid_fraction", "outer_bt_min_c",
        "inner_bt_min_c", "inner_cold_cloud_fraction",
    ),
}
NUMERIC_FIELDS: dict[str, tuple[str, ...]] = {
    "metar": ("temp", "dewp", "wdir", "wspd", "altim"),
    "open_meteo": (
        "precipitation", "rain", "showers", "weather_code", "visibility", "wind_speed_10m", "wind_gusts_10m",
    ),
    "himawari_metrics": ("b13_valid_fraction", "outer_bt_min_c", "inner_bt_min_c", "inner_cold_cloud_fraction"),
}
HOURLY_VARS = ("precipitation", "rain", "showers", "weather_code", "visibility", "wind_speed_10m", "wind_gusts_10m")


def flatten_envelope(source: str, env: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yields one flat record per METAR report / forecast hour / airport scan row."""
    if source == "metar":
        for rec in env.get("payload") or []:
            if isinstance(rec, dict):
                yield rec
    elif source == "open_meteo":
        meta = env.get("metadata") or {}
        hourly = ((env.get("payload") or {}).get("hourly")) or {}
        times = hourly.get("time") or []
        for i, ts in enumerate(times):
            rec: dict[str, Any] = {"airport_icao": meta.get("airport_icao"), "time": ts}
            for var in HOURLY_VARS:
                col = hourly.get(var)
                rec[var] = col[i] if isinstance(col, list) and i < len(col) else None
            yield rec
    elif source == "himawari_metrics":
        for a in env.get("airports") or []:
            if isinstance(a, dict):
                yield a


def _is_missing(value: Any) -> bool:
    if value is None or value == "":
        return True
    return isinstance(value, float) and math.isnan(value)


def _as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or _is_missing(value):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _fingerprint(record: dict[str, Any]) -> bytes:
    stable = {k: v for k, v in record.items() if k not in VOLATILE_FIELDS}
    return hashlib.md5(json.dumps(stable, sort_keys=True, default=str).encode("utf-8")).digest()


def profile_records(source: str, records: Iterator[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Rows for one source: a ``*`` row (rows, duplicates) plus one row per key field."""
    keys = KEY_FIELDS[source]
    numeric = set(NUMERIC_FIELDS[source])
    n = 0
    missing = {k: 0 for k in keys}
    values: dict[str, list[float]] = {k: [] for k in numeric}
    seen: set[bytes] = set()
    duplicates = 0
    for rec in records:
        n += 1
        fp = _fingerprint(rec)
        if fp in seen:
            duplicates += 1
        else:
            seen.add(fp)
        for k in keys:
            if _is_missing(rec.get(k)):
                missing[k] += 1
        for k in numeric:
            num = _as_number(rec.get(k))
            if num is not None:
                values[k].append(num)
    win = {"window_start_utc": iso_z(start), "window_end_utc": iso_z(end)}
    rows: list[dict[str, Any]] = [{
        "source": source, "field": "*", "rows": n, "missing_count": None, "missing_rate": None,
        "duplicate_rows": duplicates, "duplicate_rate": (duplicates / n) if n else None,
        "min": None, "p50": None, "max": None, **win,
    }]
    for k in keys:
        vals = values.get(k, [])
        rows.append({
            "source": source, "field": k, "rows": n, "missing_count": missing[k],
            "missing_rate": (missing[k] / n) if n else None, "duplicate_rows": None, "duplicate_rate": None,
            "min": min(vals) if vals else None,
            "p50": float(np.percentile(vals, 50)) if vals else None,
            "max": max(vals) if vals else None, **win,
        })
    return rows


def compute_profiles(storage_root: str, now: datetime, days: int = 7) -> list[dict[str, Any]]:
    """Profiles of every Bronze source over the last ``days`` days (by date partition)."""
    start = now - timedelta(days=days)
    rows: list[dict[str, Any]] = []
    for source in KEY_FIELDS:
        def records(src: str = source) -> Iterator[dict[str, Any]]:
            for env in iter_bronze(storage_root, src, start, now, pad_days=0):
                yield from flatten_envelope(src, env)

        rows.extend(profile_records(source, records(), start, now))
    return rows
