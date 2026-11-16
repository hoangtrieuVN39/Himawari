"""Robust (median/MAD) anomaly detection on per-cycle ingestion statistics.

Series per source, built from ``data/logs/ingestion_log.jsonl`` (backfilled entries excluded: they are
burst downloads and would distort the reference): ``bytes_written``, ``record_count`` and the inter-cycle
gap in minutes. Statistics use the whole retained history; rows are emitted for cycles inside the window.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import numpy as np

from src.diagnostics.health import iso_z

Z_THRESHOLD = 3.5
MIN_SAMPLES = 5
CYCLE_GROUP_SECONDS = 120.0  # entries of one daemon cycle (e.g. 3 Open-Meteo airports) land within seconds
MAD_SCALE = 0.6745  # Iglewicz-Hoaglin modified z-score
MEAN_AD_SCALE = 1.253314  # fallback when MAD == 0 (constant-ish series)


def robust_z_scores(values: list[float], min_samples: int = MIN_SAMPLES) -> tuple[list[Optional[float]], dict[str, Any]]:
    """Modified z-scores 0.6745*(x-median)/MAD; falls back to mean-absolute-deviation when MAD is 0.

    Returns (z per value, info{median, mad, method}); ``method`` is 'insufficient' (z all None) when
    fewer than ``min_samples`` values exist, 'constant' when the series has no spread (z all 0).
    """
    arr = np.asarray(values, dtype=float)
    if arr.size < min_samples:
        return [None] * arr.size, {"median": float(np.median(arr)) if arr.size else None, "mad": None, "method": "insufficient"}
    median = float(np.median(arr))
    abs_dev = np.abs(arr - median)
    mad = float(np.median(abs_dev))
    if mad > 0:
        z = MAD_SCALE * (arr - median) / mad
        method = "mad"
    else:
        mean_ad = float(np.mean(abs_dev))
        if mean_ad > 0:
            z = (arr - median) / (MEAN_AD_SCALE * mean_ad)
            method = "mean_ad"
        else:
            return [0.0] * arr.size, {"median": median, "mad": 0.0, "method": "constant"}
    return [float(x) for x in z], {"median": median, "mad": mad, "method": method}


def flag_outliers(values: list[float], threshold: float = Z_THRESHOLD, min_samples: int = MIN_SAMPLES) -> list[bool]:
    z, _ = robust_z_scores(values, min_samples)
    return [zi is not None and abs(zi) > threshold for zi in z]


def build_cycles(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Groups non-backfill log entries into per-source cycles (sorted by time)."""
    by_source: dict[str, list[dict[str, Any]]] = {}
    for e in sorted((x for x in entries if not x.get("is_backfill")), key=lambda x: x["_t"]):
        cycles = by_source.setdefault(str(e.get("source")), [])
        if cycles and (e["_t"] - cycles[-1]["start"]).total_seconds() <= CYCLE_GROUP_SECONDS:
            cyc = cycles[-1]
        else:
            cyc = {"start": e["_t"], "bytes_written": 0, "record_count": None, "n_entries": 0}
            cycles.append(cyc)
        cyc["n_entries"] += 1
        cyc["bytes_written"] += int(e.get("bytes_written") or 0)
        if e.get("record_count") is not None:
            cyc["record_count"] = (cyc["record_count"] or 0) + int(e["record_count"])
    for cycles in by_source.values():
        for prev, cur in zip(cycles, cycles[1:]):
            cur["gap_minutes"] = (cur["start"] - prev["start"]).total_seconds() / 60.0
        if cycles:
            cycles[0]["gap_minutes"] = None
    return by_source


def detect_anomalies(
    entries: list[dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
    threshold: float = Z_THRESHOLD,
) -> list[dict[str, Any]]:
    """Rows (one per source x metric x cycle in window) with robust z and the |z| > threshold flag."""
    rows: list[dict[str, Any]] = []
    for source, cycles in sorted(build_cycles(entries).items()):
        for metric in ("bytes_written", "record_count", "gap_minutes"):
            series = [(c["start"], c.get(metric)) for c in cycles if c.get(metric) is not None]
            if not series:
                continue
            z, info = robust_z_scores([float(v) for _, v in series])
            for (t, v), zi in zip(series, z):
                if not (window_start <= t <= window_end):
                    continue
                rows.append({
                    "source": source, "metric": metric, "cycle_at_utc": iso_z(t), "value": float(v),
                    "median": info["median"], "mad": info["mad"], "robust_z": zi,
                    "is_anomaly": zi is not None and abs(zi) > threshold, "method": info["method"],
                    "n_reference": len(series),
                    "window_start_utc": iso_z(window_start), "window_end_utc": iso_z(window_end),
                })
    return rows
