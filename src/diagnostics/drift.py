"""Population Stability Index (PSI) drift detection: last 24 h vs the previous 72 h.

Features: ``inner_bt_min_c`` and ``inner_cold_cloud_fraction`` (Himawari metrics), ``visibility_m`` (METAR
raw observation) and ``forecast_rain_mm`` (Open-Meteo precipitation for the hour after each fetch).
PSI > 0.2 is flagged as significant drift.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np

from src.diagnostics.health import complete_hours, finite, floor_hour, iso_z, iter_bronze, parse_utc

PSI_THRESHOLD = 0.2
PSI_BINS = 10
PSI_EPSILON = 1e-4
MIN_SAMPLES = 30
CURRENT_HOURS = 24
REFERENCE_HOURS = 72

FEATURES = ("inner_bt_min_c", "inner_cold_cloud_fraction", "visibility_m", "forecast_rain_mm")

_WIND_RE = re.compile(r"^(?:\d{3}|VRB)\d{2,3}(?:G\d{2,3})?(?:KT|MPS|KMH)$")
_VIS_RE = re.compile(r"^\d{4}$")


def psi(
    reference: list[float],
    current: list[float],
    bins: int = PSI_BINS,
    epsilon: float = PSI_EPSILON,
) -> tuple[Optional[float], int]:
    """PSI with quantile bins taken from the reference sample. Returns (psi, bins_used).

    Bin edges are the unique reference quantiles; bins are (-inf, e1], (e1, e2], ..., (ek, +inf) so every
    current value lands somewhere. Empty bins are smoothed with ``epsilon`` before taking logs.
    """
    ref = np.asarray(reference, dtype=float)
    cur = np.asarray(current, dtype=float)
    if ref.size == 0 or cur.size == 0:
        return None, 0
    qs = np.quantile(ref, np.linspace(0, 1, bins + 1)[1:-1])
    edges = np.unique(qs)
    ref_idx = np.searchsorted(edges, ref, side="left")
    cur_idx = np.searchsorted(edges, cur, side="left")
    n_bins = edges.size + 1
    p = np.bincount(ref_idx, minlength=n_bins) / ref.size
    q = np.bincount(cur_idx, minlength=n_bins) / cur.size
    p = np.maximum(p, epsilon)
    q = np.maximum(q, epsilon)
    return float(np.sum((q - p) * np.log(q / p))), int(n_bins)


def metar_visibility_m(raw_ob: Optional[str]) -> Optional[float]:
    """Prevailing visibility in metres from a METAR string (CAVOK -> 10000); None if not found."""
    if not raw_ob:
        return None
    tokens = raw_ob.split()
    if "CAVOK" in tokens:
        return 10000.0
    for i, tok in enumerate(tokens):
        if _WIND_RE.match(tok):
            for nxt in tokens[i + 1:i + 3]:
                if _VIS_RE.match(nxt):
                    return float(nxt)
            return None
    return None


def collect_drift_features(
    storage_root: str, now: datetime, lookback_hours: int = CURRENT_HOURS + REFERENCE_HOURS
) -> dict[str, list[tuple[datetime, float]]]:
    """(event time, value) samples per feature from Bronze over the lookback horizon."""
    start = now - timedelta(hours=lookback_hours)
    feats: dict[str, list[tuple[datetime, float]]] = {name: [] for name in FEATURES}

    for env in iter_bronze(storage_root, "himawari_metrics", start, now):
        for a in env.get("airports") or []:
            if str(a.get("status", "")).upper() != "AVAILABLE":
                continue
            t = parse_utc(a.get("scan_time_utc"))
            if t is None:
                continue
            for name in ("inner_bt_min_c", "inner_cold_cloud_fraction"):
                vals = finite([a.get(name)])
                if vals:
                    feats[name].append((t, vals[0]))

    seen_obs: set[tuple[Any, Any]] = set()
    for env in iter_bronze(storage_root, "metar", start, now):
        for rec in env.get("payload") or []:
            t = parse_utc(rec.get("obsTime"))
            key = (rec.get("icaoId"), rec.get("obsTime"), rec.get("rawOb"))
            if t is None or key in seen_obs:
                continue  # METAR is re-fetched with hours=2: count each observation once
            seen_obs.add(key)
            vis = metar_visibility_m(rec.get("rawOb"))
            if vis is not None:
                feats["visibility_m"].append((t, vis))

    for env in iter_bronze(storage_root, "open_meteo", start, now):
        meta = env.get("metadata") or {}
        fetched = parse_utc(meta.get("fetched_at_utc"))
        hourly = ((env.get("payload") or {}).get("hourly")) or {}
        if fetched is None or not hourly.get("time"):
            continue
        target = floor_hour(fetched) + timedelta(hours=1)
        for i, ts in enumerate(hourly["time"]):
            if parse_utc(ts) == target:
                vals = finite([(hourly.get("precipitation") or [None] * (i + 1))[i]])
                if vals:
                    feats["forecast_rain_mm"].append((fetched, vals[0]))
                break
    return feats


def compute_drift(
    features: dict[str, list[tuple[datetime, float]]],
    now: datetime,
    *,
    current_hours: int = CURRENT_HOURS,
    reference_hours: int = REFERENCE_HOURS,
    threshold: float = PSI_THRESHOLD,
    min_samples: int = MIN_SAMPLES,
) -> list[dict[str, Any]]:
    """One row per feature: PSI(current window vs preceding reference window) and the drift flag."""
    cur_start = now - timedelta(hours=current_hours)
    ref_start = cur_start - timedelta(hours=reference_hours)
    rows: list[dict[str, Any]] = []
    for name in FEATURES:
        samples = features.get(name, [])
        cur = [(t, v) for t, v in samples if cur_start <= t <= now]
        ref = [(t, v) for t, v in samples if ref_start <= t < cur_start]
        status, value, bins_used = "ok", None, 0
        if len(ref) < min_samples or len(cur) < min_samples:
            status = "insufficient_data"
        else:
            value, bins_used = psi([v for _, v in ref], [v for _, v in cur])
        covered = {floor_hour(t) for t, _ in cur}
        missing = [h for h in complete_hours(cur_start, now) if h not in covered]
        rows.append({
            "feature": name, "scope": "ALL",
            "reference_start_utc": iso_z(ref_start), "reference_end_utc": iso_z(cur_start),
            "current_start_utc": iso_z(cur_start), "current_end_utc": iso_z(now),
            "n_reference": len(ref), "n_current": len(cur), "psi": value, "threshold": threshold,
            "drift_flag": value is not None and value > threshold, "bins_used": bins_used, "status": status,
            "missing_hours_count": len(missing), "missing_hours": [iso_z(h) for h in missing],
        })
    return rows
