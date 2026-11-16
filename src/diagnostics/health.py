"""Shared primitives and pillar-1 metrics: freshness SLA, Himawari coverage, decision KPIs, cost.

Conventions (apply to every metric produced by this package):
  * ``numerator`` / ``denominator`` / ``value`` (= numerator / denominator unless ``unit`` says otherwise);
  * ``window_start_utc`` / ``window_end_utc`` (ISO "Z" strings) delimit the measured period;
  * ``missing_hours``: complete UTC hours inside the window that contributed no valid sample
    (no successful fetch, no processed scan, no qualifying decision row, ...).
"""

from __future__ import annotations

import json
import math
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.config_loader import load_airports_config
from src.ingestion.fs_utils import safe_replace
from src.ingestion.raw_store import RawEnvelopeStore
from src.spark.priority_rules import METAR_STALE_MINUTES

UTC = timezone.utc
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_AIRPORTS_CONFIG = REPO_ROOT / "config" / "airports.json"

HIMAWARI_STEP_MIN = 10
HIMAWARI_SLOTS_PER_DAY = 144
# A scan is only expected once it had time to appear on S3 and be fetched by the 10-min daemon
# (observed: slot 05:50 fetched 06:11).
HIMAWARI_AVAILABILITY_LAG_MIN = 30
SOURCE_CADENCE_MIN = {"metar": 10, "open_meteo": 60, "himawari_hsd": 10}
SLA_GRACE_FACTOR = 2.0  # fetch-age SLA = grace * cadence (one missed cycle is tolerated)
METAR_OBSERVATION_SLA_MIN = float(METAR_STALE_MINUTES)
DECISION_SLOT_KIND_LIVE = "live"

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def parse_utc(value: Any) -> Optional[datetime]:
    """Parses ISO strings (with/without Z or fraction), epoch seconds or datetimes into aware UTC."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), UTC)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def iso_z(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def floor_minutes(dt: datetime, minutes: int) -> datetime:
    total = dt.hour * 60 + dt.minute
    total -= total % minutes
    return dt.replace(hour=total // 60, minute=total % 60, second=0, microsecond=0)


def ceil_minutes(dt: datetime, minutes: int) -> datetime:
    floored = floor_minutes(dt, minutes)
    return floored if floored == dt else floored + timedelta(minutes=minutes)


def floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def complete_hours(start: datetime, end: datetime) -> list[datetime]:
    """UTC hour starts h such that [h, h+1h) lies fully inside [start, end]."""
    hours: list[datetime] = []
    h = floor_hour(start)
    if h < start:
        h += timedelta(hours=1)
    while h + timedelta(hours=1) <= end:
        hours.append(h)
        h += timedelta(hours=1)
    return hours


def utc_dates(start: datetime, end: datetime) -> list[str]:
    days: list[str] = []
    d = start.date()
    while d <= end.date():
        days.append(d.isoformat())
        d += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Metric rows
# ---------------------------------------------------------------------------


def make_metric(
    name: str,
    scope: str,
    numerator: float,
    denominator: float,
    start: datetime,
    end: datetime,
    missing_hours: Iterable[datetime] = (),
    *,
    unit: str = "ratio",
    value: Optional[float] = None,
    status: Optional[str] = None,
    detail: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """One row of the long-format ``metrics`` table."""
    missing = sorted(iso_z(h) for h in missing_hours)
    if value is None and unit == "ratio" and denominator > 0:
        value = float(numerator) / float(denominator)
    if status is None:
        status = "ok" if value is not None else "no_data"
    return {
        "name": name,
        "scope": scope,
        "numerator": float(numerator),
        "denominator": float(denominator),
        "value": None if value is None else float(value),
        "unit": unit,
        "window_start_utc": iso_z(start),
        "window_end_utc": iso_z(end),
        "missing_hours_count": len(missing),
        "missing_hours": missing,
        "status": status,
        "detail": json.dumps(detail or {}, ensure_ascii=False, default=str),
    }


# ---------------------------------------------------------------------------
# Parquet I/O (atomic overwrite)
# ---------------------------------------------------------------------------

_STR, _F64, _I64, _BOOL, _LIST = pa.string(), pa.float64(), pa.int64(), pa.bool_(), pa.list_(pa.string())


def _schema(**fields: pa.DataType) -> pa.Schema:
    return pa.schema(list(fields.items()))


_WIN = {"window_start_utc": _STR, "window_end_utc": _STR}

TABLE_SCHEMAS: dict[str, pa.Schema] = {
    "metrics": _schema(
        name=_STR, scope=_STR, numerator=_F64, denominator=_F64, value=_F64, unit=_STR,
        **_WIN, missing_hours_count=_I64, missing_hours=_LIST, status=_STR, detail=_STR,
    ),
    "freshness": _schema(
        source=_STR, scope=_STR, last_fetched_at_utc=_STR, age_minutes=_F64, cadence_minutes=_F64,
        sla_minutes=_F64, sla_breached=_BOOL, last_observation_at_utc=_STR, observation_age_minutes=_F64,
        observation_sla_minutes=_F64, observation_breached=_BOOL, status=_STR, **_WIN,
    ),
    "freshness_hourly": _schema(
        source=_STR, hour_utc=_STR, fetch_count=_I64, available=_BOOL, **_WIN,
    ),
    "himawari_coverage": _schema(
        day_utc=_STR, scope_type=_STR, scope=_STR, expected_scans=_I64, downloaded_scans=_I64,
        processed_scans=_I64, coverage_ratio=_F64, missing_scans=_I64, backfilled_scans=_I64,
        airport_rows_expected=_I64, airport_rows_available=_I64, is_partial_day=_BOOL,
        missing_hours_count=_I64, missing_hours=_LIST, **_WIN,
    ),
    "gap_attribution": _schema(
        source=_STR, scope=_STR, slot_utc=_STR, cause=_STR, local_evidence=_STR, check_method=_STR,
        airports_affected=_STR, **_WIN,
    ),
    "gap_summary": _schema(
        source=_STR, cause=_STR, count=_I64, total=_I64, share=_F64, source_checked=_BOOL, **_WIN,
    ),
    "anomalies": _schema(
        source=_STR, metric=_STR, cycle_at_utc=_STR, value=_F64, median=_F64, mad=_F64, robust_z=_F64,
        is_anomaly=_BOOL, method=_STR, n_reference=_I64, **_WIN,
    ),
    "drift": _schema(
        feature=_STR, scope=_STR, reference_start_utc=_STR, reference_end_utc=_STR,
        current_start_utc=_STR, current_end_utc=_STR, n_reference=_I64, n_current=_I64, psi=_F64,
        threshold=_F64, drift_flag=_BOOL, bins_used=_I64, status=_STR, missing_hours_count=_I64,
        missing_hours=_LIST,
    ),
    "profile": _schema(
        source=_STR, field=_STR, rows=_I64, missing_count=_I64, missing_rate=_F64, duplicate_rows=_I64,
        duplicate_rate=_F64, min=_F64, p50=_F64, max=_F64, **_WIN,
    ),
    "volume": _schema(
        kind=_STR, name=_STR, bytes=_I64, files=_I64, **_WIN,
    ),
    "pipeline_stage_timings": _schema(
        stage=_STR, n=_I64, median_s=_F64, p95_s=_F64, max_s=_F64, **_WIN,
    ),
}


def write_parquet_atomic(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    """Writes rows to ``path`` via a sibling temp file then an atomic replace (overwrites)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    tmp = path.parent / f"{path.name}.tmp.{os.getpid()}_{uuid.uuid4().hex[:6]}"
    try:
        pq.write_table(table, tmp)
        safe_replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def write_json_atomic(path: Path, payload: Any) -> None:
    from src.ingestion.fs_utils import atomic_write_bytes

    atomic_write_bytes(Path(path), json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"))


# ---------------------------------------------------------------------------
# Input readers
# ---------------------------------------------------------------------------


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Reads a JSONL file, skipping blank/corrupt lines (the daemon appends concurrently)."""
    path = Path(path)
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def load_ingestion_log(storage_root: str | Path) -> list[dict[str, Any]]:
    """Ingestion log entries with parsed ``_t`` (fetched_at_utc) and ``_slot`` (expected_slot_utc)."""
    entries = read_jsonl(Path(storage_root) / "logs" / "ingestion_log.jsonl")
    out: list[dict[str, Any]] = []
    for e in entries:
        t = parse_utc(e.get("fetched_at_utc"))
        if t is None:
            continue
        e["_t"] = t
        e["_slot"] = parse_utc(e.get("expected_slot_utc"))
        out.append(e)
    out.sort(key=lambda e: e["_t"])
    return out


def iter_bronze(
    storage_root: str | Path, source: str, start: datetime, end: datetime, pad_days: int = 1
) -> Iterator[dict[str, Any]]:
    """Bronze envelopes for ``source`` whose date partition lies in [start-pad, end+pad] (UTC days)."""
    store = RawEnvelopeStore(str(storage_root))
    pad = timedelta(days=pad_days)
    for day in utc_dates(start - pad, end + pad):
        yield from store.iter_envelopes(source, date_str=day, strict=False)


def load_airports(config_path: Optional[str | Path] = None) -> dict[str, dict[str, Any]]:
    return load_airports_config(str(config_path or DEFAULT_AIRPORTS_CONFIG))


def read_heartbeat(storage_root: str | Path, now: datetime) -> dict[str, Any]:
    """Runner heartbeat staleness; ``present`` is False when the file is absent/unreadable."""
    path = Path(storage_root) / "lake" / "_runner_heartbeat.json"
    result: dict[str, Any] = {"present": False, "path": str(path)}
    if not path.is_file():
        return result
    try:
        hb = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return result
    completed = parse_utc(hb.get("last_cycle_completed_at_utc"))
    started = parse_utc(hb.get("last_cycle_started_at_utc"))
    cycle_s = hb.get("cycle_seconds")
    try:
        cycle_s_f = float(cycle_s) if cycle_s is not None else None
    except (TypeError, ValueError):
        cycle_s_f = None
    threshold_min = max(15.0, 3.0 * (cycle_s_f or 0.0) / 60.0)
    reference = completed or started
    age_min = (now - reference).total_seconds() / 60.0 if reference else None
    result.update(
        present=True,
        status=hb.get("status"),
        last_cycle_started_at_utc=iso_z(started),
        last_cycle_completed_at_utc=iso_z(completed),
        cycle_seconds=cycle_s_f,
        age_minutes=age_min,
        stale_threshold_minutes=threshold_min,
        is_stale=age_min is None or age_min > threshold_min,
    )
    return result


# ---------------------------------------------------------------------------
# Himawari evidence + coverage
# ---------------------------------------------------------------------------


@dataclass
class HimawariEvidence:
    """What the receiver knows about each 10-min scan slot (all keys are aware UTC datetimes)."""

    segments: list[str]
    airport_segment: dict[str, str]
    processed_airports: dict[datetime, set[str]] = field(default_factory=dict)
    processed_segments: dict[datetime, set[str]] = field(default_factory=dict)
    downloaded_segments: dict[datetime, set[str]] = field(default_factory=dict)
    attempted: set[datetime] = field(default_factory=set)
    error_segments: dict[datetime, set[str]] = field(default_factory=dict)
    backfill_processed: set[datetime] = field(default_factory=set)

    def first_slot(self) -> Optional[datetime]:
        slots = set(self.processed_segments) | set(self.attempted) | set(self.downloaded_segments)
        return min(slots) if slots else None


def required_segments(airports_cfg: dict[str, dict[str, Any]]) -> tuple[list[str], dict[str, str]]:
    airport_segment = {icao: cfg.get("himawari_segment", "S0410") for icao, cfg in airports_cfg.items()}
    return sorted(set(airport_segment.values())), airport_segment


def collect_himawari_evidence(
    storage_root: str | Path,
    airports_cfg: dict[str, dict[str, Any]],
    log_entries: list[dict[str, Any]],
    start: datetime,
    end: datetime,
) -> HimawariEvidence:
    """Merges Bronze ``himawari_metrics`` envelopes, the ingestion log and the file-error log."""
    segments, airport_segment = required_segments(airports_cfg)
    ev = HimawariEvidence(segments=segments, airport_segment=airport_segment)

    for env in iter_bronze(storage_root, "himawari_metrics", start, end):
        meta = env.get("metadata") or {}
        slot = parse_utc(meta.get("scan_slot_utc"))
        if slot is None:
            continue
        for a in env.get("airports") or []:
            if str(a.get("status", "")).upper() != "AVAILABLE":
                continue
            icao = a.get("airport_icao")
            seg = a.get("segment_code") or airport_segment.get(icao)
            if icao:
                ev.processed_airports.setdefault(slot, set()).add(icao)
            if seg:
                ev.processed_segments.setdefault(slot, set()).add(seg)
            if meta.get("is_backfill"):
                ev.backfill_processed.add(slot)

    for e in log_entries:
        if e.get("source") != "himawari_hsd":
            continue
        slot = e.get("_slot")
        if slot is None:
            continue
        ev.attempted.add(slot)
        for seg in (e.get("files") or {}):
            ev.downloaded_segments.setdefault(slot, set()).add(seg)

    for rec in read_jsonl(Path(storage_root) / "logs" / "himawari_file_errors.jsonl"):
        if rec.get("is_synthetic_test"):
            continue
        slot = parse_utc(rec.get("scan_slot_utc"))
        seg = rec.get("segment")
        if slot is not None and seg:
            ev.error_segments.setdefault(slot, set()).add(seg)
    return ev


def expected_himawari_slots(start: datetime, end: datetime, now: datetime) -> list[datetime]:
    """10-min scan slots in [start, end] that should already be available at ``now``."""
    cutoff = floor_minutes(now - timedelta(minutes=HIMAWARI_AVAILABILITY_LAG_MIN), HIMAWARI_STEP_MIN)
    last = min(end, cutoff)
    slots: list[datetime] = []
    t = ceil_minutes(start, HIMAWARI_STEP_MIN)
    while t <= last:
        slots.append(t)
        t += timedelta(minutes=HIMAWARI_STEP_MIN)
    return slots


def _scope_checks(ev: HimawariEvidence, scope_type: str, scope: str) -> tuple[Callable[[datetime], bool], Callable[[datetime], bool], int]:
    """(is_processed, is_downloaded, airports_in_scope) predicates for one coverage scope."""
    if scope_type == "all_segments":
        need = set(ev.segments)
        n_airports = len(ev.airport_segment)
        return (
            lambda s: need <= ev.processed_segments.get(s, set()),
            lambda s: need <= ev.downloaded_segments.get(s, set()),
            n_airports,
        )
    if scope_type == "segment":
        n_airports = sum(1 for seg in ev.airport_segment.values() if seg == scope)
        return (
            lambda s: scope in ev.processed_segments.get(s, set()),
            lambda s: scope in ev.downloaded_segments.get(s, set()),
            n_airports,
        )
    seg = ev.airport_segment.get(scope)
    return (
        lambda s: scope in ev.processed_airports.get(s, set()),
        lambda s: seg in ev.downloaded_segments.get(s, set()),
        1,
    )


def coverage_stats(ev: HimawariEvidence, slots: list[datetime], scope_type: str, scope: str) -> dict[str, Any]:
    """Counts expected/downloaded/processed scans for one scope plus fully-missing hours."""
    is_processed, is_downloaded, n_airports = _scope_checks(ev, scope_type, scope)
    processed = [s for s in slots if is_processed(s)]
    downloaded = [s for s in slots if is_downloaded(s)]
    by_hour: dict[datetime, list[int]] = {}
    for s in slots:
        counts = by_hour.setdefault(floor_hour(s), [0, 0])
        counts[0] += 1
        counts[1] += 1 if is_processed(s) else 0
    missing_hours = sorted(h for h, (exp, done) in by_hour.items() if exp > 0 and done == 0)
    airport_available = 0
    if scope_type == "airport":
        airport_available = len(processed)
    else:
        for s in slots:
            present = ev.processed_airports.get(s, set())
            if scope_type == "all_segments":
                airport_available += len(present & set(ev.airport_segment))
            else:
                airport_available += sum(1 for a in present if ev.airport_segment.get(a) == scope)
    return {
        "expected": len(slots),
        "downloaded": len(downloaded),
        "processed": len(processed),
        "missing_slots": [s for s in slots if not is_processed(s)],
        "missing_hours": missing_hours,
        "backfilled": sum(1 for s in processed if s in ev.backfill_processed),
        "airport_rows_expected": len(slots) * n_airports,
        "airport_rows_available": airport_available,
    }


def himawari_scopes(ev: HimawariEvidence) -> list[tuple[str, str]]:
    return (
        [("all_segments", "ALL")]
        + [("segment", seg) for seg in ev.segments]
        + [("airport", icao) for icao in sorted(ev.airport_segment)]
    )


def compute_himawari_coverage(
    ev: HimawariEvidence,
    now: datetime,
    window_start: datetime,
    window_end: datetime,
    *,
    max_days: int = 31,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Per-UTC-day coverage rows (144 expected scans per segment per full day) and window metrics."""
    first = ev.first_slot()
    first_day = (first if first and first < window_start else window_start).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    first_day = max(first_day, window_end.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=max_days - 1))
    day_rows: list[dict[str, Any]] = []
    day = first_day
    while day <= window_end:
        day_end = day + timedelta(days=1)
        slots = expected_himawari_slots(day, day_end - timedelta(seconds=1), now)
        is_partial = len(slots) < HIMAWARI_SLOTS_PER_DAY
        for scope_type, scope in himawari_scopes(ev):
            st = coverage_stats(ev, slots, scope_type, scope)
            day_rows.append({
                "day_utc": day.date().isoformat(),
                "scope_type": scope_type,
                "scope": scope,
                "expected_scans": st["expected"],
                "downloaded_scans": st["downloaded"],
                "processed_scans": st["processed"],
                "coverage_ratio": (st["processed"] / st["expected"]) if st["expected"] else None,
                "missing_scans": st["expected"] - st["processed"],
                "backfilled_scans": st["backfilled"],
                "airport_rows_expected": st["airport_rows_expected"],
                "airport_rows_available": st["airport_rows_available"],
                "is_partial_day": is_partial,
                "missing_hours_count": len(st["missing_hours"]),
                "missing_hours": [iso_z(h) for h in st["missing_hours"]],
                "window_start_utc": iso_z(day),
                "window_end_utc": iso_z(min(day_end, window_end)),
            })
        day = day_end

    window_slots = expected_himawari_slots(window_start, window_end, now)
    metrics: list[dict[str, Any]] = []
    for scope_type, scope in himawari_scopes(ev):
        st = coverage_stats(ev, window_slots, scope_type, scope)
        label = "ALL" if scope_type == "all_segments" else scope
        metrics.append(make_metric(
            "himawari_scan_coverage", label, st["processed"], st["expected"],
            window_start, window_end, st["missing_hours"],
            detail={"scope_type": scope_type, "downloaded": st["downloaded"], "backfilled": st["backfilled"],
                    "airport_rows_available": st["airport_rows_available"],
                    "airport_rows_expected": st["airport_rows_expected"]},
        ))
        if scope_type in ("all_segments", "segment"):
            metrics.append(make_metric(
                "himawari_download_coverage", label, st["downloaded"], st["expected"],
                window_start, window_end, [],
                detail={"scope_type": scope_type},
            ))
    return day_rows, metrics


# ---------------------------------------------------------------------------
# Freshness SLA
# ---------------------------------------------------------------------------


def _age_min(now: datetime, t: Optional[datetime]) -> Optional[float]:
    return None if t is None else (now - t).total_seconds() / 60.0


def compute_freshness(
    entries: list[dict[str, Any]],
    metar_latest_obs: dict[str, datetime],
    himawari_latest_scan: dict[str, datetime],
    now: datetime,
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Returns (freshness rows, hourly availability rows, metrics).

    Fetch SLA = SLA_GRACE_FACTOR x cadence; METAR additionally needs observation age <= 60 min per airport.
    Backfilled log entries are ignored: they describe past data, not receiver liveness.
    """
    live = [e for e in entries if not e.get("is_backfill")]
    hours = complete_hours(start, end)
    rows: list[dict[str, Any]] = []
    hourly: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    win = {"window_start_utc": iso_z(start), "window_end_utc": iso_z(end)}

    def success(e: dict[str, Any]) -> bool:
        return str(e.get("status", "")).upper() == "SUCCESS"

    for source, cadence in SOURCE_CADENCE_MIN.items():
        ok = [e for e in live if e.get("source") == source and success(e)]
        last = max((e["_t"] for e in ok), default=None)
        age = _age_min(now, last)
        sla = cadence * SLA_GRACE_FACTOR
        obs_all = None
        if source == "metar" and metar_latest_obs:
            obs_all = max(metar_latest_obs.values())
        elif source == "himawari_hsd" and himawari_latest_scan:
            obs_all = max(himawari_latest_scan.values())
        obs_age = _age_min(now, obs_all)
        obs_sla = METAR_OBSERVATION_SLA_MIN if source == "metar" else None
        breached = age is None or age > sla
        obs_breached = (obs_age is None or obs_age > obs_sla) if obs_sla is not None else None
        rows.append({
            "source": source, "scope": "ALL", "last_fetched_at_utc": iso_z(last), "age_minutes": age,
            "cadence_minutes": float(cadence), "sla_minutes": sla, "sla_breached": breached,
            "last_observation_at_utc": iso_z(obs_all), "observation_age_minutes": obs_age,
            "observation_sla_minutes": obs_sla, "observation_breached": obs_breached,
            "status": "no_data" if last is None else ("breach" if (breached or obs_breached) else "ok"), **win,
        })

        per_airport: dict[str, Optional[datetime]] = {}
        obs_map: dict[str, datetime] = {}
        if source == "open_meteo":
            for e in ok:
                icao = e.get("airport")
                if icao and icao != "ALL":
                    per_airport[icao] = max(per_airport.get(icao) or e["_t"], e["_t"])
        elif source == "metar":
            obs_map = metar_latest_obs
            per_airport = {icao: None for icao in obs_map}
        elif source == "himawari_hsd":
            obs_map = himawari_latest_scan
            per_airport = {icao: None for icao in obs_map}
        for icao in sorted(per_airport):
            fetched = per_airport[icao]
            f_age = _age_min(now, fetched)
            f_breach = None if fetched is None else f_age > sla
            o = obs_map.get(icao)
            o_age = _age_min(now, o)
            o_breach = (o_age is None or o_age > obs_sla) if obs_sla is not None else None
            rows.append({
                "source": source, "scope": icao, "last_fetched_at_utc": iso_z(fetched), "age_minutes": f_age,
                "cadence_minutes": float(cadence), "sla_minutes": sla if fetched is not None else None,
                "sla_breached": f_breach, "last_observation_at_utc": iso_z(o), "observation_age_minutes": o_age,
                "observation_sla_minutes": obs_sla, "observation_breached": o_breach,
                "status": "breach" if (f_breach or o_breach) else "ok", **win,
            })

        counts: dict[datetime, int] = {}
        for e in ok:
            counts[floor_hour(e["_t"])] = counts.get(floor_hour(e["_t"]), 0) + 1
        missing = []
        for h in hours:
            n = counts.get(h, 0)
            hourly.append({"source": source, "hour_utc": iso_z(h), "fetch_count": n, "available": n > 0, **win})
            if n == 0:
                missing.append(h)
        metrics.append(make_metric(
            "hourly_availability", source, len(hours) - len(missing), len(hours), start, end, missing,
            detail={"cadence_minutes": cadence},
        ))
        if age is not None:
            metrics.append(make_metric(
                "fetch_age_minutes", source, age, sla, start, end, [], unit="minutes", value=age,
                status="breach" if breached else "ok", detail={"sla_minutes": sla},
            ))
    return rows, hourly, metrics


def latest_metar_obs(storage_root: str | Path, now: datetime, lookback_hours: int = 48) -> dict[str, datetime]:
    """Latest METAR ``obsTime`` per airport from Bronze (observation time, not fetch time)."""
    latest: dict[str, datetime] = {}
    for env in iter_bronze(storage_root, "metar", now - timedelta(hours=lookback_hours), now):
        for rec in env.get("payload") or []:
            t = parse_utc(rec.get("obsTime"))
            icao = rec.get("icaoId")
            if t is not None and icao and t <= now + timedelta(minutes=5) and t > latest.get(icao, t - timedelta(seconds=1)):
                latest[icao] = t
    return latest


def latest_himawari_scans(ev: HimawariEvidence) -> dict[str, datetime]:
    latest: dict[str, datetime] = {}
    for slot, airports in ev.processed_airports.items():
        for icao in airports:
            if icao not in latest or slot > latest[icao]:
                latest[icao] = slot
    return latest


# ---------------------------------------------------------------------------
# Decision-table KPIs + pipeline latency
# ---------------------------------------------------------------------------


def _quantile(values: list[float], q: float) -> Optional[float]:
    return float(np.percentile(np.asarray(values, dtype=float), q)) if values else None


def _kpi_metrics(rows: list[dict[str, Any]], scope: str, start: datetime, end: datetime, detail: dict[str, Any]) -> list[dict[str, Any]]:
    hours = complete_hours(start, end)

    def qualifies_full(r: dict[str, Any]) -> bool:
        return bool(r.get("metar_is_present")) and bool(r.get("forecast_is_present")) and bool(r.get("b13_is_present"))

    def qualifies_fresh(r: dict[str, Any]) -> bool:
        age = r.get("metar_age_minutes")
        try:
            return bool(r.get("metar_is_present")) and age is not None and float(age) <= METAR_STALE_MINUTES
        except (TypeError, ValueError):
            return False

    checks = {
        "decision_full_evidence_share": qualifies_full,
        "decision_fresh_metar_share": qualifies_fresh,
        "decision_full_evidence_fresh_share": lambda r: qualifies_full(r) and qualifies_fresh(r),
    }
    out = []
    for name, fn in checks.items():
        good_hours = {floor_hour(parse_utc(r["review_slot_utc"])) for r in rows if fn(r)}
        out.append(make_metric(
            name, scope, sum(1 for r in rows if fn(r)), len(rows), start, end,
            [h for h in hours if h not in good_hours], detail=detail,
        ))
    return out


def compute_decision_kpis(
    slots: dict[str, list[dict[str, Any]]], window_start: datetime, window_end: datetime
) -> list[dict[str, Any]]:
    """Evidence/freshness shares over the window and over all time.

    When the Gold column ``slot_kind`` exists only ``live`` rows count; otherwise all (replay) rows do.
    """
    rows = [r for group in slots.values() for r in group if parse_utc(r.get("review_slot_utc")) is not None]
    has_kind = any("slot_kind" in r for r in rows)
    if has_kind:
        rows = [r for r in rows if r.get("slot_kind") == DECISION_SLOT_KIND_LIVE]
    detail = {"slot_kind": DECISION_SLOT_KIND_LIVE if has_kind else "all(replay)", "rows_total": len(rows)}

    in_window = [r for r in rows if window_start <= parse_utc(r["review_slot_utc"]) <= window_end]
    metrics = _kpi_metrics(in_window, "window", window_start, window_end, {**detail, "rows_total": len(in_window)})
    if rows:
        times = [parse_utc(r["review_slot_utc"]) for r in rows]
        metrics += _kpi_metrics(rows, "all_time", min(times), max(times), detail)
    else:
        metrics += _kpi_metrics([], "all_time", window_start, window_end, detail)
    return metrics


def load_pipeline_runs(storage_root: str | Path) -> list[dict[str, Any]]:
    runs = read_jsonl(Path(storage_root) / "logs" / "pipeline_runs.jsonl")
    for r in runs:
        r["_completed"] = parse_utc(r.get("completed_at_utc"))
    return [r for r in runs if r["_completed"] is not None]


def compute_pipeline_latency(
    runs: list[dict[str, Any]], start: datetime, end: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """P50/P95 of per-run latency_s (completed_at - latest_inputs_complete_at) + median per stage."""
    in_window = [r for r in runs if start <= r["_completed"] <= end]
    samples = [float(r["latency_s"]) for r in in_window if isinstance(r.get("latency_s"), (int, float))]
    done_hours = {floor_hour(r["_completed"]) for r in in_window}
    missing = [h for h in complete_hours(start, end) if h not in done_hours]
    metrics = []
    for name, q in (("pipeline_latency_p50_s", 50), ("pipeline_latency_p95_s", 95)):
        value = _quantile(samples, q)
        metrics.append(make_metric(
            name, "ALL", len(samples), len(in_window), start, end, missing, unit="seconds", value=value,
            status="ok" if value is not None else "not_available",
            detail={"definition": "completed_at_utc - latest_inputs_complete_at_utc per Spark run"},
        ))
    stage_rows: list[dict[str, Any]] = []
    stages: dict[str, list[float]] = {}
    for r in in_window:
        for stage, secs in (r.get("timings_s") or {}).items():
            if isinstance(secs, (int, float)):
                stages.setdefault(stage, []).append(float(secs))
    for stage, vals in stages.items():
        stage_rows.append({
            "stage": stage, "n": len(vals), "median_s": _quantile(vals, 50), "p95_s": _quantile(vals, 95),
            "max_s": max(vals), "window_start_utc": iso_z(start), "window_end_utc": iso_z(end),
        })
    return metrics, stage_rows


# ---------------------------------------------------------------------------
# Cost / volume
# ---------------------------------------------------------------------------


def _walk_size(path: Path) -> tuple[int, int]:
    total = files = 0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                            files += 1
                    except OSError:
                        continue  # file vanished (daemon cleanup / atomic replace)
        except OSError:
            continue
    return total, files


def compute_volume(
    storage_root: str | Path, entries: list[dict[str, Any]], start: datetime, end: datetime
) -> list[dict[str, Any]]:
    """Bytes downloaded in the window per source (from ingestion_log) and bytes retained per data/ area."""
    win = {"window_start_utc": iso_z(start), "window_end_utc": iso_z(end)}
    rows: list[dict[str, Any]] = []
    by_source: dict[str, list[int]] = {}
    for e in entries:
        if start <= e["_t"] <= end:
            agg = by_source.setdefault(f"{e.get('source')}{'_backfill' if e.get('is_backfill') else ''}", [0, 0])
            agg[0] += int(e.get("bytes_written") or 0)
            agg[1] += 1 if int(e.get("bytes_written") or 0) > 0 else 0
    for name, (b, n) in sorted(by_source.items()):
        rows.append({"kind": "downloaded_window", "name": name, "bytes": b, "files": n, **win})
    total_bytes = total_files = 0
    root = Path(storage_root)
    if root.is_dir():
        loose = [0, 0]
        for child in sorted(root.iterdir()):
            if child.is_dir():
                b, n = _walk_size(child)
                rows.append({"kind": "retained", "name": child.name, "bytes": b, "files": n, **win})
                total_bytes += b
                total_files += n
            elif child.is_file():
                try:
                    loose[0] += child.stat().st_size
                    loose[1] += 1
                except OSError:
                    pass
        if loose[1]:
            rows.append({"kind": "retained", "name": "(root files)", "bytes": loose[0], "files": loose[1], **win})
            total_bytes += loose[0]
            total_files += loose[1]
    rows.append({"kind": "retained_total", "name": "ALL", "bytes": total_bytes, "files": total_files, **win})
    return rows


def finite(values: Iterable[Any]) -> list[float]:
    out: list[float] = []
    for v in values:
        if isinstance(v, bool) or v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            out.append(f)
    return out
