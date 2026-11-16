"""Gap attribution: S3 existence check decides RECEIVER_FAULT vs SOURCE_MISSING."""

from datetime import datetime, timedelta, timezone

from src.diagnostics.gap_attribution import (
    RECEIVER_FAULT,
    SOURCE_MISSING,
    UNKNOWN,
    S3HeadChecker,
    classify_cycle_gaps,
    classify_himawari_gaps,
    detect_cycle_gaps,
    summarize_gaps,
)
from src.diagnostics.health import HimawariEvidence

UTC = timezone.utc
SEGMENTS = ["S0410", "S0510"]
AIRPORT_SEGMENT = {"VVDN": "S0410", "VVNB": "S0410", "VVTS": "S0510"}
START = datetime(2026, 11, 10, 0, 0, tzinfo=UTC)
END = datetime(2026, 11, 10, 2, 0, tzinfo=UTC)
MISSING_SLOT = datetime(2026, 11, 10, 1, 0, tzinfo=UTC)


def _evidence(process_all_but_missing=True) -> HimawariEvidence:
    ev = HimawariEvidence(segments=SEGMENTS, airport_segment=AIRPORT_SEGMENT)
    t = START
    while t <= END:
        if not (process_all_but_missing and t == MISSING_SLOT):
            ev.processed_segments[t] = set(SEGMENTS)
            ev.processed_airports[t] = set(AIRPORT_SEGMENT)
        t += timedelta(minutes=10)
    return ev


def _slots():
    return [START + timedelta(minutes=10 * i) for i in range(13)]


def test_missing_slot_with_file_on_s3_is_receiver_fault():
    rows = classify_himawari_gaps(_evidence(), _slots(), START, END, checker=lambda slot, seg: True)
    assert {(r["slot_utc"], r["scope"]) for r in rows} == {("2026-11-10T01:00:00Z", "S0410"), ("2026-11-10T01:00:00Z", "S0510")}
    assert {r["cause"] for r in rows} == {RECEIVER_FAULT}
    assert all(r["check_method"] == "source_check" for r in rows)


def test_missing_slot_absent_on_s3_is_source_missing():
    rows = classify_himawari_gaps(_evidence(), _slots(), START, END, checker=lambda slot, seg: False)
    assert {r["cause"] for r in rows} == {SOURCE_MISSING}


def test_per_segment_check_and_inconclusive_results():
    def checker(slot, seg):
        return {"S0410": True, "S0510": False}[seg]

    by_scope = {r["scope"]: r["cause"] for r in classify_himawari_gaps(_evidence(), _slots(), START, END, checker)}
    assert by_scope == {"S0410": RECEIVER_FAULT, "S0510": SOURCE_MISSING}
    rows = classify_himawari_gaps(_evidence(), _slots(), START, END, checker=lambda s, g: None)
    assert {r["cause"] for r in rows} == {UNKNOWN} and rows[0]["check_method"] == "source_check_inconclusive"


def test_without_checker_gap_is_unknown_but_local_download_proves_receiver_fault():
    ev = _evidence()
    ev.attempted.add(MISSING_SLOT)
    rows = {r["scope"]: r for r in classify_himawari_gaps(ev, _slots(), START, END, checker=None)}
    assert rows["S0410"]["cause"] == UNKNOWN and rows["S0410"]["local_evidence"] == "download_failed"

    ev.downloaded_segments[MISSING_SLOT] = {"S0410"}  # downloaded but never processed
    called = []
    rows = {r["scope"]: r for r in classify_himawari_gaps(ev, _slots(), START, END, checker=lambda s, g: called.append(g) or False)}
    assert rows["S0410"]["cause"] == RECEIVER_FAULT and rows["S0410"]["local_evidence"] == "downloaded_not_processed"
    assert rows["S0510"]["cause"] == SOURCE_MISSING
    assert called == ["S0510"]  # no network check wasted on a file we already hold


def test_no_gap_rows_when_everything_processed():
    assert classify_himawari_gaps(_evidence(process_all_but_missing=False), _slots(), START, END, checker=lambda s, g: True) == []


def test_max_checks_cap_leaves_oldest_unknown():
    ev = HimawariEvidence(segments=SEGMENTS, airport_segment=AIRPORT_SEGMENT)  # nothing processed
    rows = classify_himawari_gaps(ev, _slots()[:3], START, END, checker=lambda s, g: True, max_checks=2)
    assert sum(1 for r in rows if r["cause"] == UNKNOWN) == 4


def test_s3_head_checker_maps_http_status():
    class Resp:
        def __init__(self, code):
            self.status_code = code

    class Session:
        def __init__(self, code):
            self.code, self.urls = code, []

        def head(self, url, **kwargs):
            self.urls.append(url)
            return Resp(self.code)

    s200, s404, s403 = Session(200), Session(404), Session(403)
    assert S3HeadChecker(session=s200)(MISSING_SLOT, "S0410") is True
    assert S3HeadChecker(session=s404)(MISSING_SLOT, "S0410") is False
    assert S3HeadChecker(session=s403)(MISSING_SLOT, "S0410") is None
    assert s200.urls[0].endswith("AHI-L1b-FLDK/2026/11/10/0100/HS_H09_20261110_0100_B13_FLDK_R20_S0410.DAT.bz2")


def test_cycle_gap_detection_flags_daemon_downtime():
    times = [START + timedelta(minutes=10 * i) for i in range(4)] + [START + timedelta(minutes=70 + 10 * i) for i in range(5)]
    missing = detect_cycle_gaps(times, START, START + timedelta(minutes=110), 10)
    assert [m - START for m in missing] == [timedelta(minutes=m) for m in (40, 50, 60)]
    assert detect_cycle_gaps([START + timedelta(minutes=10 * i + 1) for i in range(12)], START, START + timedelta(minutes=120), 10) == []


def test_classify_cycle_gaps_separates_receiver_fault_from_failed_fetch():
    times = [START + timedelta(minutes=m) for m in (5, 15, 25, 85, 95, 105, 115)]
    entries = [
        {"source": "metar", "_t": t, "status": "FAILED" if i == 4 else "SUCCESS", "error_type": "ConnectionError", "is_backfill": False}
        for i, t in enumerate(times)
    ]
    rows, stats = classify_cycle_gaps(entries, "metar", START, END)
    causes = [(r["cause"], r["local_evidence"]) for r in rows]
    assert causes.count((RECEIVER_FAULT, "no_cycle_logged")) == stats["missing"] == 5
    assert (SOURCE_MISSING, "fetch_failed:ConnectionError") in causes
    assert stats["expected"] == 12 and stats["observed"] == 7 and stats["failed"] == 1
    summary = {(s["source"], s["cause"]): s["count"] for s in summarize_gaps(rows, START, END, True)}
    assert summary[("metar", RECEIVER_FAULT)] == 5 and summary[("metar", SOURCE_MISSING)] == 1
