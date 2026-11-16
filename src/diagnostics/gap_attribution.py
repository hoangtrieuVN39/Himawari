"""Gap attribution: why is an expected observation missing?

Himawari (per expected 10-min slot x required segment):
  * ``RECEIVER_FAULT``  the file exists on S3 (we downloaded it, or the source check says so) but was not
    processed locally (daemon down, download/processing error);
  * ``SOURCE_MISSING``  the file is absent on S3 (source check says so);
  * ``UNKNOWN``         no local proof and no source check (run without ``--check-source``).
METAR / Open-Meteo (per expected ingestion cycle): a cycle with no log entry at all means the daemon was
not running (``RECEIVER_FAULT``); a fetch that failed is ``SOURCE_MISSING``; a METAR cycle that fetched fine
but brought no newer observation is counted separately (``FETCHED_NO_NEW_OBSERVATION``, normal for hourly METAR).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

import requests

from src.diagnostics.health import (
    SOURCE_CADENCE_MIN,
    HimawariEvidence,
    complete_hours,
    expected_himawari_slots,
    floor_hour,
    iso_z,
    iter_bronze,
    make_metric,
    parse_utc,
)
from src.ingestion.himawari_client import build_s3_segment_url
from src.ingestion.http_client import DEFAULT_USER_AGENT

SOURCE_MISSING = "SOURCE_MISSING"
RECEIVER_FAULT = "RECEIVER_FAULT"
UNKNOWN = "UNKNOWN"
FETCHED_NO_NEW_OBSERVATION = "FETCHED_NO_NEW_OBSERVATION"

GAP_TOLERANCE = 1.5  # a gap > 1.5 x cadence hides at least one missing cycle
DEFAULT_MAX_SOURCE_CHECKS = 600

# (slot, segment) -> True: file exists on S3; False: absent; None: could not tell.
SourceChecker = Callable[[datetime, str], Optional[bool]]


class S3HeadChecker:
    """Existence check of a Himawari-9 B13 segment on the public NOAA bucket via HTTPS HEAD."""

    def __init__(self, timeout: float = 10.0, session: Optional[requests.Session] = None) -> None:
        self.timeout = timeout
        self.session = session or requests.Session()

    def __call__(self, slot: datetime, segment: str) -> Optional[bool]:
        url, _ = build_s3_segment_url(slot, segment)
        try:
            resp = self.session.head(url, timeout=self.timeout, headers={"User-Agent": DEFAULT_USER_AGENT})
        except requests.RequestException:
            return None
        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            return False
        return None  # 403/5xx: neither proof of presence nor of absence


def classify_himawari_gaps(
    ev: HimawariEvidence,
    slots: list[datetime],
    window_start: datetime,
    window_end: datetime,
    checker: Optional[SourceChecker] = None,
    max_checks: int = DEFAULT_MAX_SOURCE_CHECKS,
) -> list[dict[str, Any]]:
    """One row per missing (slot, segment) with its cause. Newest slots are checked first under the cap."""
    win = {"window_start_utc": iso_z(window_start), "window_end_utc": iso_z(window_end)}
    pending: list[dict[str, Any]] = []
    for slot in slots:
        have = ev.processed_segments.get(slot, set())
        for seg in ev.segments:
            if seg in have:
                continue
            affected = sorted(a for a, s in ev.airport_segment.items() if s == seg)
            if seg in ev.downloaded_segments.get(slot, set()) or seg in ev.error_segments.get(slot, set()):
                # we hold (or choked on) the file => it existed on S3; no network check needed
                cause, evidence, method = RECEIVER_FAULT, "downloaded_not_processed", "local_download"
                if seg not in ev.downloaded_segments.get(slot, set()):
                    evidence = "processing_error"
            else:
                cause, method = UNKNOWN, "none"
                evidence = "download_failed" if slot in ev.attempted else "no_attempt"
            pending.append({
                "source": "himawari", "scope": seg, "slot_utc": iso_z(slot), "cause": cause,
                "local_evidence": evidence, "check_method": method, "airports_affected": ",".join(affected),
                "_slot": slot, **win,
            })

    if checker is not None:
        to_check = [r for r in pending if r["cause"] == UNKNOWN]
        to_check.sort(key=lambda r: r["_slot"], reverse=True)
        to_check = to_check[:max_checks]
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda r: checker(r["_slot"], r["scope"]), to_check))
        for row, exists in zip(to_check, results):
            row["check_method"] = "source_check"
            if exists is True:
                row["cause"] = RECEIVER_FAULT
            elif exists is False:
                row["cause"] = SOURCE_MISSING
            else:
                row["check_method"] = "source_check_inconclusive"
    for row in pending:
        del row["_slot"]
    return pending


def detect_cycle_gaps(
    fetch_times: list[datetime], start: datetime, end: datetime, cadence_min: int, tolerance: float = GAP_TOLERANCE
) -> list[datetime]:
    """Estimated times of missing ingestion cycles: inter-fetch gaps (window edges included) > tolerance x cadence."""
    points = [start] + sorted(t for t in fetch_times if start <= t <= end) + [end]
    missing: list[datetime] = []
    for a, b in zip(points, points[1:]):
        gap = (b - a).total_seconds() / 60.0
        if gap > tolerance * cadence_min:
            n = max(int(round(gap / cadence_min)) - 1, 1)
            missing.extend(a + timedelta(minutes=cadence_min * k) for k in range(1, n + 1))
    return missing


def classify_cycle_gaps(
    entries: list[dict[str, Any]], source: str, start: datetime, end: datetime
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Gap rows for a cycle-based source (metar, open_meteo) + completeness stats.

    Live (non-backfill) log entries only. Returns (rows, stats{expected, observed, missing, failed}).
    """
    cadence = SOURCE_CADENCE_MIN[source]
    win = {"window_start_utc": iso_z(start), "window_end_utc": iso_z(end)}
    mine = [e for e in entries if e.get("source") == source and not e.get("is_backfill") and start <= e["_t"] <= end]
    # one cycle may log several entries (one per airport): collapse entries closer than half a cadence
    times: list[datetime] = []
    for t in sorted(e["_t"] for e in mine):
        if not times or (t - times[-1]).total_seconds() > cadence * 30:
            times.append(t)
    missing = detect_cycle_gaps(times, start, end, cadence)
    rows: list[dict[str, Any]] = [{
        "source": source, "scope": "ALL", "slot_utc": iso_z(t), "cause": RECEIVER_FAULT,
        "local_evidence": "no_cycle_logged", "check_method": "ingestion_log", "airports_affected": "ALL", **win,
    } for t in missing]
    failed = [e for e in mine if str(e.get("status", "")).upper() == "FAILED"]
    for e in failed:
        rows.append({
            "source": source, "scope": str(e.get("airport") or "ALL"), "slot_utc": iso_z(e["_t"]),
            "cause": SOURCE_MISSING, "local_evidence": f"fetch_failed:{e.get('error_type')}",
            "check_method": "ingestion_log", "airports_affected": str(e.get("airport") or "ALL"), **win,
        })
    expected = max(int(round((end - start).total_seconds() / 60.0 / cadence)), 0)
    return rows, {"expected": expected, "observed": len(times), "missing": len(missing), "failed": len(failed)}


def metar_cycle_observation_stats(
    storage_root: str, start: datetime, end: datetime
) -> dict[str, Any]:
    """Fetched METAR cycles in the window that did / did not advance the newest ``obsTime`` seen so far."""
    cycles: list[tuple[datetime, Optional[datetime]]] = []
    for env in iter_bronze(storage_root, "metar", start, end):
        meta = env.get("metadata") or {}
        fetched = parse_utc(meta.get("fetched_at_utc"))
        if fetched is None or meta.get("is_backfill"):
            continue
        obs = [t for t in (parse_utc(r.get("obsTime")) for r in env.get("payload") or []) if t is not None]
        cycles.append((fetched, max(obs) if obs else None))
    cycles.sort(key=lambda c: c[0])
    newest: Optional[datetime] = None
    fetched_n = new_n = 0
    for fetched, obs in cycles:
        advanced = obs is not None and (newest is None or obs > newest)
        if obs is not None and (newest is None or obs > newest):
            newest = obs
        if start <= fetched <= end:
            fetched_n += 1
            new_n += 1 if advanced else 0
    return {"fetched_cycles": fetched_n, "with_new_observation": new_n, "no_new_observation": fetched_n - new_n}


def summarize_gaps(
    rows: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    source_checked: bool,
    metar_obs: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    """gap_summary rows: count and share per (source, cause)."""
    win = {"window_start_utc": iso_z(start), "window_end_utc": iso_z(end)}
    out: list[dict[str, Any]] = []
    for source in sorted({r["source"] for r in rows}):
        src_rows = [r for r in rows if r["source"] == source]
        for cause in (RECEIVER_FAULT, SOURCE_MISSING, UNKNOWN):
            n = sum(1 for r in src_rows if r["cause"] == cause)
            out.append({
                "source": source, "cause": cause, "count": n, "total": len(src_rows),
                "share": (n / len(src_rows)) if src_rows else None,
                "source_checked": source_checked if source == "himawari" else True, **win,
            })
    if metar_obs:
        fetched = metar_obs["fetched_cycles"]
        out.append({
            "source": "metar", "cause": FETCHED_NO_NEW_OBSERVATION, "count": metar_obs["no_new_observation"],
            "total": fetched, "share": (metar_obs["no_new_observation"] / fetched) if fetched else None,
            "source_checked": True, **win,
        })
    return out


def gap_metrics(
    rows: list[dict[str, Any]],
    himawari_slots: list[datetime],
    cycle_stats: dict[str, dict[str, Any]],
    start: datetime,
    end: datetime,
    fetch_times: dict[str, list[datetime]],
) -> list[dict[str, Any]]:
    """Long-format metrics: share of expected Himawari slots with every segment present, cycle completeness."""
    metrics: list[dict[str, Any]] = []
    missing_slots = {parse_utc(r["slot_utc"]) for r in rows if r["source"] == "himawari"}
    done_hours = {floor_hour(s) for s in himawari_slots if s not in missing_slots}
    metrics.append(make_metric(
        "himawari_gap_free_slots", "ALL", len(himawari_slots) - len(missing_slots), len(himawari_slots),
        start, end, [h for h in complete_hours(start, end) if h not in done_hours],
        detail={"missing_slots": len(missing_slots)},
    ))
    for source, st in cycle_stats.items():
        hours_with = {floor_hour(t) for t in fetch_times.get(source, [])}
        metrics.append(make_metric(
            "ingest_cycle_completeness", source, max(st["expected"] - st["missing"], 0), st["expected"],
            start, end, [h for h in complete_hours(start, end) if h not in hours_with],
            detail={"observed_cycles": st["observed"], "missing_cycles": st["missing"], "failed_fetches": st["failed"]},
        ))
    return metrics
