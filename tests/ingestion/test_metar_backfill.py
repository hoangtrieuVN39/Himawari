import json
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from src.ingestion.metar_backfill import (
    ENTRIES_PER_HOUR_BOUND,
    MIN_REQUEST_INTERVAL_SEC,
    backfill_metar,
    max_window_hours,
    split_windows,
    window_batch_id,
)
from src.ingestion.metar_client import METAR_MAX_ENTRIES_PER_REQUEST, ingest_metar
from src.ingestion.raw_store import RawEnvelopeStore
from src.ingestion.run_ingest import parse_args

NOW = datetime(2026, 11, 16, 6, 12, 33, tzinfo=timezone.utc)


def _utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def _response(payload, status=200, url="https://aviationweather.gov/api/data/metar"):
    resp = Mock()
    resp.status_code = status
    resp.url = url
    resp.content = b"" if status == 204 else json.dumps(payload).encode("utf-8")
    if status == 204:
        resp.json.side_effect = ValueError("no body on 204")
    else:
        resp.json.return_value = payload
    return resp


class FakeAwc:
    """Fetcher stub: one observation per request, tagged with the requested window (+ optional mutation)."""

    def __init__(self, clock=None, tag="v1", empty_dates=(), fail_dates=()):
        self.calls = []
        self.starts = []
        self.clock = clock
        self.tag = tag
        self.empty_dates = set(empty_dates)
        self.fail_dates = set(fail_dates)

    def __call__(self, url, params=None, **kwargs):
        self.calls.append(dict(params))
        if self.clock is not None:
            self.starts.append(self.clock())
        if params["date"] in self.fail_dates:
            raise RuntimeError("boom")
        if params["date"] in self.empty_dates:
            return _response([], status=204)
        payload = [{
            "icaoId": params["ids"],
            "obsTime": 1,
            "reportTime": params["date"],
            "rawOb": f"METAR {params['ids']} {params['date']} {self.tag}",
        }]
        return _response(payload, url=f"{url}?ids={params['ids']}&date={params['date']}")


class FakeClock:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock():
    return FakeClock()


def _run(tmp_path, clock, fetcher, airports=("VVDN", "VVNB"), days=2, **kwargs):
    return backfill_metar(
        airports=list(airports),
        days=days,
        base_dir=str(tmp_path),
        now_utc=NOW,
        fetcher=fetcher,
        sleeper=clock.sleep,
        clock=clock,
        **kwargs,
    )


def _log_entries(tmp_path):
    lines = (tmp_path / "logs" / "ingestion_log.jsonl").read_text(encoding="utf-8").strip().splitlines()
    return [json.loads(line) for line in lines]


# ---------------------------------------------------------------- window splitting


def test_default_window_size_is_safely_under_the_entry_limit():
    assert max_window_hours() * ENTRIES_PER_HOUR_BOUND + 1 <= METAR_MAX_ENTRIES_PER_REQUEST
    assert 24 <= max_window_hours()


def test_split_windows_30_days_stay_under_limit_and_are_contiguous():
    end = _utc(2026, 11, 16, 6)
    start = end - timedelta(days=30)
    windows = split_windows(start, end)

    assert len(windows) == 31
    assert windows[0][0] <= start < windows[0][1]
    assert windows[-1][1] == end
    for (ws, we), nxt in zip(windows, windows[1:] + [None]):
        hours = (we - ws) / timedelta(hours=1)
        assert 1 <= hours <= 24
        assert hours * ENTRIES_PER_HOUR_BOUND + 1 <= METAR_MAX_ENTRIES_PER_REQUEST
        if nxt:
            assert we == nxt[0]
    # midnight alignment => window_end does not depend on the run time
    assert all(ws.hour == 0 and ws.minute == 0 for ws, _ in windows)


def test_split_windows_rejects_windows_that_could_exceed_limit():
    with pytest.raises(ValueError):
        split_windows(_utc(2026, 11, 1), _utc(2026, 11, 10), window_hours=max_window_hours() + 1)
    with pytest.raises(ValueError):
        split_windows(_utc(2026, 11, 1), _utc(2026, 11, 10), window_hours=0)


def test_split_windows_empty_range():
    assert split_windows(_utc(2026, 11, 1, 5), _utc(2026, 11, 1, 5)) == []


def test_window_batch_id_is_deterministic_per_airport_and_window_end():
    we = _utc(2026, 11, 15)
    assert window_batch_id("vvdn", we) == window_batch_id("VVDN", we) == "20261115T000000Z_metar_backfill_VVDN"
    assert window_batch_id("VVNB", we) != window_batch_id("VVDN", we)


# ---------------------------------------------------------------- orchestration


def test_backfill_requests_one_airport_per_window_with_date_and_hours(tmp_path, clock):
    fetcher = FakeAwc()
    summary = _run(tmp_path, clock, fetcher)

    assert summary["requests"] == 6  # 3 windows x 2 airports
    assert {c["ids"] for c in fetcher.calls} == {"VVDN", "VVNB"}
    assert all("," not in c["ids"] for c in fetcher.calls)
    assert all(c["format"] == "json" and int(c["hours"]) <= 24 for c in fetcher.calls)
    vvdn_dates = [c["date"] for c in fetcher.calls if c["ids"] == "VVDN"]
    assert vvdn_dates == ["2026-11-15T00:00:00Z", "2026-11-16T00:00:00Z", "2026-11-16T06:00:00Z"]
    partial = [c for c in fetcher.calls if c["date"] == "2026-11-16T06:00:00Z"][0]
    assert partial["hours"] == "6"

    envelopes = list(RawEnvelopeStore(storage_root=str(tmp_path)).iter_envelopes("metar"))
    assert len(envelopes) == 6
    meta = next(e["metadata"] for e in envelopes if e["metadata"]["batch_id"] == "20261115T000000Z_metar_backfill_VVDN")
    assert meta["is_backfill"] is True
    assert meta["backfill_window_start_utc"] == "2026-11-14T00:00:00Z"
    assert meta["backfill_window_end_utc"] == "2026-11-15T00:00:00Z"
    assert meta["airports_requested"] == ["VVDN"]
    assert meta["record_count"] == 1
    assert summary["records_by_airport"] == {"VVDN": 3, "VVNB": 3}


def test_backfill_logs_each_request_with_window_fields(tmp_path, clock):
    _run(tmp_path, clock, FakeAwc())
    entries = _log_entries(tmp_path)
    assert len(entries) == 6
    entry = entries[0]
    for key in ("batch_id", "source", "airport", "expected_slot_utc", "fetched_at_utc", "status", "http_status",
                "record_count", "bytes_written", "file_path", "is_backfill", "error_type", "error_message",
                "backfill_window_start_utc", "backfill_window_end_utc"):
        assert key in entry
    assert entry["source"] == "metar" and entry["airport"] == "VVDN"
    assert entry["status"] == "SUCCESS" and entry["is_backfill"] is True and entry["bytes_written"] > 0


def test_throttle_enforces_min_interval_between_requests(tmp_path, clock):
    fetcher = FakeAwc(clock=clock)
    _run(tmp_path, clock, fetcher)

    assert len(fetcher.starts) == 6
    gaps = [b - a for a, b in zip(fetcher.starts, fetcher.starts[1:])]
    assert all(gap >= MIN_REQUEST_INTERVAL_SEC - 1e-9 for gap in gaps)
    assert len(clock.sleeps) == 5  # no sleep before the first request


def test_throttle_skips_sleep_when_requests_are_already_spaced(tmp_path, clock):
    class SlowFetcher(FakeAwc):
        def __call__(self, url, params=None, **kwargs):
            clock.now += 2.0  # each request itself takes longer than the interval
            return super().__call__(url, params=params)

    _run(tmp_path, clock, SlowFetcher())
    assert clock.sleeps == []


def test_rerun_is_a_noop_thanks_to_deterministic_batch_ids(tmp_path, clock):
    first = _run(tmp_path, clock, FakeAwc())
    store = RawEnvelopeStore(storage_root=str(tmp_path))
    before = [e["metadata"]["batch_id"] for e in store.iter_envelopes("metar")]
    size_before = sum(p.stat().st_size for p in (tmp_path / "raw_compact" / "metar").rglob("*.jsonl"))

    second = _run(tmp_path, clock, FakeAwc())

    after = [e["metadata"]["batch_id"] for e in store.iter_envelopes("metar")]
    size_after = sum(p.stat().st_size for p in (tmp_path / "raw_compact" / "metar").rglob("*.jsonl"))
    assert first["inserted"] == 6 and first["noop"] == 0
    assert second["inserted"] == 0 and second["noop"] == 6 and second["bytes_written"] == 0
    assert before == after and len(after) == len(set(after))
    assert size_before == size_after


def test_http_204_is_no_data_not_an_error(tmp_path, clock):
    fetcher = FakeAwc(empty_dates={"2026-11-15T00:00:00Z"})
    summary = _run(tmp_path, clock, fetcher, airports=("VVDN",))

    assert summary["no_data"] == 1 and summary["failed"] == 0
    assert summary["inserted"] == 2  # the other two windows are stored
    statuses = [e["status"] for e in _log_entries(tmp_path)]
    assert statuses.count("NO_DATA") == 1
    no_data = next(e for e in _log_entries(tmp_path) if e["status"] == "NO_DATA")
    assert no_data["http_status"] == 204 and no_data["record_count"] == 0 and no_data["error_type"] is None


def test_payload_conflict_is_logged_and_skipped_without_crashing(tmp_path, clock):
    _run(tmp_path, clock, FakeAwc(tag="v1"))
    summary = _run(tmp_path, clock, FakeAwc(tag="v2"))  # same batch ids, different payloads

    assert summary["conflicts"] == 6 and summary["inserted"] == 0 and summary["failed"] == 0
    assert [e["status"] for e in _log_entries(tmp_path)[-6:]] == ["SKIPPED_CONFLICT"] * 6
    envelopes = list(RawEnvelopeStore(storage_root=str(tmp_path)).iter_envelopes("metar"))
    assert len(envelopes) == 6
    assert all(env["payload"][0]["rawOb"].endswith("v1") for env in envelopes)


def test_request_failure_does_not_abort_the_run(tmp_path, clock):
    fetcher = FakeAwc(fail_dates={"2026-11-15T00:00:00Z"})
    summary = _run(tmp_path, clock, fetcher)

    assert summary["requests"] == 6
    assert summary["failed"] == 2  # both airports, one window each
    assert summary["inserted"] == 4
    failed = [e for e in _log_entries(tmp_path) if e["status"] == "FAILED"]
    assert len(failed) == 2 and failed[0]["error_type"] == "RuntimeError"


@pytest.mark.parametrize("days", [0, 31, -1, 2.5, True])
def test_backfill_rejects_days_outside_1_to_30(tmp_path, clock, days):
    with pytest.raises(ValueError):
        _run(tmp_path, clock, FakeAwc(), days=days)


# ---------------------------------------------------------------- CLI + live client 204


def test_cli_backfill_days_parsing():
    assert parse_args([]).backfill_days is None
    assert parse_args(["--backfill-days", "30"]).backfill_days == 30
    for bad in ("0", "31", "abc"):
        with pytest.raises(SystemExit):
            parse_args(["--backfill-days", bad])


def test_live_ingest_metar_treats_204_as_empty_success(tmp_path, mocker):
    mocker.patch("src.ingestion.metar_client.fetch_with_retry", return_value=_response([], status=204))
    result = ingest_metar(airports=["VVDN"], hours=2, base_dir=str(tmp_path))
    assert result["status"] == "SUCCESS" and result["record_count"] == 0 and result["http_status"] == 204
