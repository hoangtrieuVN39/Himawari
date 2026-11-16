"""Robust z-score (median/MAD) anomaly detection."""

from datetime import datetime, timedelta, timezone

from src.diagnostics.anomaly import build_cycles, detect_anomalies, flag_outliers, robust_z_scores

UTC = timezone.utc


def test_mad_flags_injected_outlier_and_not_normal_points():
    normal = [100.0, 102.0, 98.0, 101.0, 99.0, 103.0, 97.0, 100.0, 101.0, 99.0]
    values = normal + [400.0]
    flags = flag_outliers(values)
    assert flags[-1] is True
    assert not any(flags[:-1])
    z, info = robust_z_scores(values)
    assert info["method"] == "mad"
    assert abs(z[-1]) > 3.5 and all(abs(zi) <= 3.5 for zi in z[:-1])


def test_constant_series_with_one_outlier_uses_mean_ad_fallback():
    values = [5.0] * 20 + [50.0]
    flags = flag_outliers(values)
    assert flags[-1] is True and not any(flags[:-1])
    _, info = robust_z_scores(values)
    assert info["method"] == "mean_ad"


def test_fully_constant_and_too_short_series_never_flag():
    assert not any(flag_outliers([7.0] * 12))
    z, info = robust_z_scores([1.0, 2.0, 3.0])
    assert z == [None, None, None] and info["method"] == "insufficient"
    assert not any(flag_outliers([1.0, 2.0, 3.0, 500.0]))


def _entry(source, t, bytes_written=1000, record_count=None, backfill=False):
    return {"source": source, "_t": t, "bytes_written": bytes_written, "record_count": record_count, "is_backfill": backfill}


def test_cycles_group_airports_and_exclude_backfill():
    t0 = datetime(2026, 11, 16, 0, 0, tzinfo=UTC)
    entries = []
    for i in range(3):
        base = t0 + timedelta(hours=i)
        entries += [_entry("open_meteo", base + timedelta(seconds=s), 100, 48) for s in (0, 1, 2)]
    entries.append(_entry("open_meteo", t0 + timedelta(hours=1, minutes=5), 999999, 1, backfill=True))
    cycles = build_cycles(entries)["open_meteo"]
    assert len(cycles) == 3
    assert [c["bytes_written"] for c in cycles] == [300, 300, 300]
    assert [c["record_count"] for c in cycles] == [144, 144, 144]
    assert cycles[1]["gap_minutes"] == 60.0


def test_detect_anomalies_flags_gap_and_bytes_outlier_inside_window():
    t0 = datetime(2026, 11, 16, 0, 0, tzinfo=UTC)
    entries = [_entry("metar", t0 + timedelta(minutes=10 * i), 1000 + (i % 3) * 10, 12) for i in range(30)]
    entries.append(_entry("metar", t0 + timedelta(minutes=10 * 30 + 120), 90000, 12))  # 2 h gap + huge payload
    rows = detect_anomalies(entries, t0, t0 + timedelta(hours=10))
    flagged = {(r["metric"], r["cycle_at_utc"]) for r in rows if r["is_anomaly"]}
    outlier_at = (t0 + timedelta(minutes=420)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert ("bytes_written", outlier_at) in flagged
    assert ("gap_minutes", outlier_at) in flagged
    assert not any(r["is_anomaly"] for r in rows if r["cycle_at_utc"] != outlier_at)
