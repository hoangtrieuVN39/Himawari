"""Pillar-1 metrics: coverage, freshness, decision KPIs, pipeline latency, CLI outputs."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.diagnostics import health as h
from src.diagnostics.run_health import main, run_health
from src.ingestion.raw_store import RawEnvelopeStore

UTC = timezone.utc
SEGMENTS = ["S0410", "S0510"]
AIRPORT_SEGMENT = {"VVDN": "S0410", "VVNB": "S0410", "VVTS": "S0510"}


def _dt(*args):
    return datetime(*args, tzinfo=UTC)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- coverage

def _fixture_evidence():
    ev = h.HimawariEvidence(segments=SEGMENTS, airport_segment=AIRPORT_SEGMENT)
    day = _dt(2026, 11, 10)
    for i in range(6):  # 00:00-00:50: every segment processed for every airport
        slot = day + timedelta(minutes=10 * i)
        ev.processed_segments[slot] = set(SEGMENTS)
        ev.processed_airports[slot] = set(AIRPORT_SEGMENT)
        ev.downloaded_segments[slot] = set(SEGMENTS)
    one = day + timedelta(hours=1)  # 01:00: only S0410 processed (VVTS missing)
    ev.processed_segments[one] = {"S0410"}
    ev.processed_airports[one] = {"VVDN", "VVNB"}
    ev.downloaded_segments[one] = {"S0410", "S0510"}  # S0510 downloaded but not processed
    ev.backfill_processed.add(day)
    return ev


def test_coverage_counts_on_fixture_day():
    now = _dt(2026, 11, 12)
    days, metrics = h.compute_himawari_coverage(_fixture_evidence(), now, _dt(2026, 11, 10), now)
    row = {(r["day_utc"], r["scope"]): r for r in days}
    allr = row[("2026-11-10", "ALL")]
    assert allr["scope_type"] == "all_segments"
    assert (allr["expected_scans"], allr["downloaded_scans"], allr["processed_scans"]) == (144, 7, 6)
    assert allr["missing_scans"] == 138 and allr["backfilled_scans"] == 1 and not allr["is_partial_day"]
    assert allr["airport_rows_expected"] == 432 and allr["airport_rows_available"] == 6 * 3 + 2
    assert allr["missing_hours_count"] == 23  # only hour 00 has a fully processed scan
    s0410, s0510, vvts = row[("2026-11-10", "S0410")], row[("2026-11-10", "S0510")], row[("2026-11-10", "VVTS")]
    assert (s0410["expected_scans"], s0410["processed_scans"]) == (144, 7)
    assert (s0510["processed_scans"], s0510["downloaded_scans"]) == (6, 7)
    assert (vvts["processed_scans"], vvts["airport_rows_expected"]) == (6, 144)
    assert row[("2026-11-11", "ALL")]["processed_scans"] == 0
    assert row[("2026-11-12", "ALL")]["expected_scans"] == 0  # nothing expected yet at 00:00 (availability lag)

    win = next(m for m in metrics if m["name"] == "himawari_scan_coverage" and m["scope"] == "ALL")
    assert (win["numerator"], win["denominator"]) == (6, 286)  # 10-Nov 00:00 ... 11-Nov 23:30
    assert win["window_start_utc"] == "2026-11-10T00:00:00Z" and win["window_end_utc"] == "2026-11-12T00:00:00Z"
    assert win["missing_hours_count"] == len(win["missing_hours"]) == 47


def test_partial_current_day_expects_only_available_slots():
    now = _dt(2026, 11, 10, 6, 5)
    days, _ = h.compute_himawari_coverage(_fixture_evidence(), now, now - timedelta(hours=24), now)
    today = next(r for r in days if r["day_utc"] == "2026-11-10" and r["scope"] == "ALL")
    assert today["is_partial_day"] and today["expected_scans"] == 34  # 00:00..05:30 inclusive


# ---------------------------------------------------------------- freshness

def test_freshness_sla_breach_and_hourly_availability():
    now = _dt(2026, 11, 16, 12)
    start = now - timedelta(hours=6)

    def e(source, minutes_ago, **kw):
        return {"source": source, "_t": now - timedelta(minutes=minutes_ago), "status": "SUCCESS", "is_backfill": False, **kw}

    entries = [e("metar", m) for m in (5, 15, 25, 75)] + [e("open_meteo", 150, airport="VVDN"), e("himawari_hsd", 45)]
    obs = {"VVDN": now - timedelta(minutes=90), "VVNB": now - timedelta(minutes=20)}
    rows, hourly, metrics = h.compute_freshness(entries, obs, {}, now, start, now)
    by = {(r["source"], r["scope"]): r for r in rows}
    assert by[("metar", "ALL")]["sla_breached"] is False
    assert by[("metar", "VVDN")]["observation_breached"] is True and by[("metar", "VVNB")]["observation_breached"] is False
    assert by[("metar", "ALL")]["status"] == "ok"
    assert by[("open_meteo", "ALL")]["sla_breached"] is True and by[("open_meteo", "ALL")]["age_minutes"] == 150
    assert by[("himawari_hsd", "ALL")]["sla_breached"] is True
    avail = next(m for m in metrics if m["name"] == "hourly_availability" and m["scope"] == "metar")
    assert (avail["numerator"], avail["denominator"]) == (2, 6) and avail["missing_hours_count"] == 4
    assert sum(1 for r in hourly if r["source"] == "metar" and r["available"]) == 2


# ---------------------------------------------------------------- decision KPIs + latency

def _row(slot, kind=None, metar=True, forecast=True, b13=True, age=10.0):
    r = {"review_slot_utc": _iso(slot), "airport_icao": "VVDN", "metar_is_present": metar,
         "metar_age_minutes": age, "forecast_is_present": forecast, "b13_is_present": b13}
    if kind:
        r["slot_kind"] = kind
    return r


def test_decision_kpis_shares_and_slot_kind_filter():
    t0 = _dt(2026, 11, 16, 0)
    rows = [_row(t0 + timedelta(minutes=5), "live"), _row(t0 + timedelta(minutes=10), "live", forecast=False),
            _row(t0 + timedelta(minutes=15), "live", age=90.0), _row(t0 + timedelta(hours=1, minutes=5), "live", b13=False),
            _row(t0 + timedelta(minutes=20), "replay")]  # replay row must be ignored once slot_kind exists
    slots = {r["review_slot_utc"]: [r] for r in rows}
    metrics = {(m["name"], m["scope"]): m for m in h.compute_decision_kpis(slots, t0, t0 + timedelta(hours=3))}
    full = metrics[("decision_full_evidence_share", "window")]
    assert (full["numerator"], full["denominator"]) == (2, 4)
    assert metrics[("decision_fresh_metar_share", "window")]["numerator"] == 3
    both = metrics[("decision_full_evidence_fresh_share", "window")]
    assert both["numerator"] == 1 and both["value"] == pytest.approx(0.25)
    assert both["missing_hours_count"] == 2  # hours 01 (no qualifying row) and 02 (no rows)
    assert json.loads(full["detail"])["slot_kind"] == "live"

    plain = {(m["name"], m["scope"]): m for m in h.compute_decision_kpis({"a": [_row(t0, None)]}, t0, t0 + timedelta(hours=1))}
    assert plain[("decision_full_evidence_share", "window")]["denominator"] == 1  # no slot_kind column -> all rows


def test_pipeline_latency_percentiles_and_stage_timings():
    now = _dt(2026, 11, 16, 12)
    runs = []
    for i, lat in enumerate([10, 20, 30, 40, 50, 60, 70, 80, 90, 100]):
        runs.append({"_completed": now - timedelta(minutes=10 * i + 1), "latency_s": lat,
                     "timings_s": {"session_init": 5.0, "write": float(i), "total": 20.0 + i}})
    runs.append({"_completed": now - timedelta(minutes=3), "latency_s": None, "timings_s": {}})
    runs.append({"_completed": now - timedelta(days=3), "latency_s": 9999, "timings_s": {"total": 999}})  # outside window
    metrics, stages = h.compute_pipeline_latency(runs, now - timedelta(hours=24), now)
    p50, p95 = {m["name"]: m for m in metrics}["pipeline_latency_p50_s"], {m["name"]: m for m in metrics}["pipeline_latency_p95_s"]
    assert p50["value"] == pytest.approx(55.0) and p95["value"] == pytest.approx(95.5)
    assert (p50["numerator"], p50["denominator"]) == (10, 11) and p50["unit"] == "seconds"
    total = next(s for s in stages if s["stage"] == "total")
    assert total["n"] == 10 and total["median_s"] == pytest.approx(24.5)

    empty, no_stages = h.compute_pipeline_latency([], now - timedelta(hours=24), now)
    assert all(m["status"] == "not_available" and m["value"] is None for m in empty) and no_stages == []


# ---------------------------------------------------------------- end to end

def _write_fixture_storage(root: Path, now: datetime) -> None:
    store = RawEnvelopeStore(str(root))
    logs = root / "logs"
    logs.mkdir(parents=True)
    log_lines = []
    slot = _dt(now.year, now.month, now.day, now.hour) - timedelta(hours=2)
    for i in range(6):
        t = slot + timedelta(minutes=10 * i)
        fetched = t + timedelta(minutes=12)
        log_lines.append({"batch_id": f"m{i}", "source": "metar", "airport": "ALL", "fetched_at_utc": _iso(fetched),
                          "expected_slot_utc": _iso(fetched), "status": "SUCCESS", "record_count": 12,
                          "bytes_written": 1000 + i, "is_backfill": False})
        files = {"S0410": "a", "S0510": "b"}
        log_lines.append({"batch_id": f"h{i}", "source": "himawari_hsd", "expected_slot_utc": _iso(t),
                          "fetched_at_utc": _iso(fetched), "status": "SUCCESS", "files": files,
                          "bytes_written": 5_000_000, "is_backfill": False})
        store.write("himawari_metrics", t.strftime("%Y-%m-%d"), {
            "metadata": {"batch_id": f"hm{i}", "scan_slot_utc": _iso(t), "is_backfill": False},
            "airports": [
                {"airport_icao": icao, "segment_code": seg, "scan_time_utc": _iso(t), "status": "AVAILABLE",
                 "inner_bt_min_c": 5.0 + i, "inner_cold_cloud_fraction": 0.0, "b13_valid_fraction": 1.0}
                for icao, seg in AIRPORT_SEGMENT.items()
            ],
        })
    (logs / "ingestion_log.jsonl").write_text("\n".join(json.dumps(x) for x in log_lines) + "\n", encoding="utf-8")
    run_ts = now - timedelta(minutes=30)
    (logs / "pipeline_runs.jsonl").write_text(json.dumps({
        "run_id": "r1", "completed_at_utc": _iso(run_ts), "latency_s": 42.0,
        "timings_s": {"session_init": 3.0, "write": 1.0, "total": 9.0}}) + "\n", encoding="utf-8")
    store.write("metar", now.strftime("%Y-%m-%d"), {
        "metadata": {"batch_id": "mb1", "fetched_at_utc": _iso(now - timedelta(minutes=5)), "is_backfill": False},
        "payload": [{"icaoId": "VVDN", "obsTime": int((now - timedelta(minutes=20)).timestamp()),
                     "rawOb": "METAR VVDN 160600Z 13010KT 9999 FEW020 30/25 Q1011", "temp": 30}],
    })


@pytest.fixture
def storage(tmp_path):
    now = _dt(2026, 11, 16, 12, 0)
    root = tmp_path / "data"
    _write_fixture_storage(root, now)
    return root, now


def test_run_health_writes_tables_and_summary(storage):
    root, now = storage
    summary = run_health(str(root), 24, False, now=now)
    out = root / "lake" / "gold" / "health"
    for name in h.TABLE_SCHEMAS:
        assert (out / f"{name}.parquet").is_file(), name
    assert not list(out.glob("*.tmp*"))
    assert json.loads((out / "summary.json").read_text(encoding="utf-8"))["generated_at_utc"] == "2026-11-16T12:00:00Z"
    assert summary["errors"] == []

    cov = pq.read_table(out / "himawari_coverage.parquet").to_pylist()
    today = next(r for r in cov if r["day_utc"] == "2026-11-16" and r["scope"] == "ALL")
    assert today["processed_scans"] == 6 and today["downloaded_scans"] == 6
    gaps = pq.read_table(out / "gap_attribution.parquet").to_pylist()
    assert {g["cause"] for g in gaps if g["source"] == "himawari"} == {"UNKNOWN"}  # no --check-source

    metrics = {(m["name"], m["scope"]): m for m in summary["metrics"]}
    assert metrics[("pipeline_latency_p50_s", "ALL")]["value"] == 42.0
    assert metrics[("decision_full_evidence_share", "window")]["status"] == "no_data"  # no decision table
    fresh = {(r["source"], r["scope"]): r for r in summary["freshness"]}
    assert fresh[("metar", "VVDN")]["observation_age_minutes"] == pytest.approx(20.0)

    # second run overwrites atomically (same files, no duplicates)
    run_health(str(root), 24, False, now=now + timedelta(minutes=1))
    assert len(pq.read_table(out / "metrics.parquet")) == len(summary["metrics"])


def test_run_health_with_injected_source_checker(storage):
    root, now = storage
    summary = run_health(str(root), 24, True, now=now, checker=lambda slot, seg: seg == "S0410")
    gaps = pq.read_table(root / "lake" / "gold" / "health" / "gap_attribution.parquet").to_pylist()
    causes = {g["cause"] for g in gaps if g["source"] == "himawari"}
    assert causes == {"RECEIVER_FAULT", "SOURCE_MISSING"}
    assert summary["source_check"] is True


def test_cli_main_prints_and_returns_zero(storage, capsys):
    root, _ = storage
    assert main(["--storage-root", str(root), "--window-hours", "6"]) == 0
    assert "himawari_scan_coverage[ALL]" in capsys.readouterr().out


def test_heartbeat_staleness(tmp_path):
    now = _dt(2026, 11, 16, 12)
    assert h.read_heartbeat(tmp_path, now) == {"present": False, "path": str(tmp_path / "lake" / "_runner_heartbeat.json")}
    hb = tmp_path / "lake" / "_runner_heartbeat.json"
    hb.parent.mkdir(parents=True)
    hb.write_text(json.dumps({"last_cycle_started_at_utc": _iso(now - timedelta(minutes=50)),
                              "last_cycle_completed_at_utc": _iso(now - timedelta(minutes=45)),
                              "status": "idle", "cycle_seconds": 60}), encoding="utf-8")
    res = h.read_heartbeat(tmp_path, now)
    assert res["present"] and res["is_stale"] and res["age_minutes"] == pytest.approx(45.0)
