import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.spark import himawari_batch as hb
from src.spark.himawari_batch import (
    list_slots,
    process_slot_partition,
    required_segments,
    summarize_slot_results,
)

AIRPORTS_CFG = {
    "VVDN": {"icao": "VVDN", "himawari_segment": "S0410"},
    "VVNB": {"icao": "VVNB", "himawari_segment": "S0410"},
    "VVTS": {"icao": "VVTS", "himawari_segment": "S0510"},
}


def _slot_dir(base: Path, slot_iso: str) -> Path:
    dt = datetime.strptime(slot_iso, "%Y-%m-%dT%H:%M:%SZ")
    return base / "raw" / "himawari_hsd" / f"date={dt:%Y-%m-%d}" / f"slot={dt:%H%M}"


class FakePipeline:
    """Stands in for run_himawari_slot_pipeline: 'downloads' two files per requested segment set."""

    def __init__(self, base: Path, fail_slots=()):
        self.base = base
        self.calls = []
        self.fail_slots = set(fail_slots)

    def __call__(self, scan_dt_utc, airports_cfg, base_dir, is_backfill, retain_raw_hours, pinned_slots):
        slot_iso = scan_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        segments = required_segments(airports_cfg)
        self.calls.append({"slot": slot_iso, "segments": segments, "is_backfill": is_backfill,
                           "pinned_slots": list(pinned_slots), "base_dir": base_dir})
        if slot_iso in self.fail_slots:
            raise RuntimeError("satpy exploded")
        slot_dir = _slot_dir(Path(base_dir), slot_iso)
        slot_dir.mkdir(parents=True, exist_ok=True)
        files = {}
        for seg in segments:
            path = slot_dir / f"HS_H09_{scan_dt_utc:%Y%m%d_%H%M}_B13_FLDK_R20_{seg}.DAT.bz2"
            path.write_bytes(b"x" * 100)
            files[seg] = str(path)
        return {
            "scan_slot_utc": slot_iso,
            "download": {"status": "SUCCESS", "files": files, "bytes_written": 100 * len(files),
                         "source_slot_utc": slot_iso},
            "metrics": {"airports": [{"airport_icao": icao, "status": "AVAILABLE"} for icao in AIRPORTS_CFG]},
            "status": "SUCCESS",
        }


def _all_available(slot_dt, segments):
    return {seg: True for seg in segments}


# ------------------------------------------------------------------ slot listing


def test_list_slots_covers_window_inclusive_at_10_minute_steps():
    slots = list_slots("2026-11-15T06:00:00Z", "2026-11-15T07:00:00Z")
    assert slots == [
        "2026-11-15T06:00:00Z", "2026-11-15T06:10:00Z", "2026-11-15T06:20:00Z", "2026-11-15T06:30:00Z",
        "2026-11-15T06:40:00Z", "2026-11-15T06:50:00Z", "2026-11-15T07:00:00Z",
    ]


def test_list_slots_rounds_unaligned_bounds_inward_to_the_scan_grid():
    slots = list_slots("2026-11-15T06:03:00Z", "2026-11-15T06:37:00+00:00")
    assert slots == ["2026-11-15T06:10:00Z", "2026-11-15T06:20:00Z", "2026-11-15T06:30:00Z"]


def test_list_slots_crosses_midnight_and_counts_a_full_day():
    slots = list_slots("2026-11-14T23:50:00Z", "2026-11-15T00:10:00Z")
    assert slots == ["2026-11-14T23:50:00Z", "2026-11-15T00:00:00Z", "2026-11-15T00:10:00Z"]
    assert len(list_slots("2026-11-14T00:00:00Z", "2026-11-14T23:50:00Z")) == 144


def test_list_slots_empty_when_end_before_first_slot():
    assert list_slots("2026-11-15T06:01:00Z", "2026-11-15T06:09:00Z") == []


def test_required_segments_derived_from_config_includes_both_segments():
    assert required_segments(AIRPORTS_CFG) == ["S0410", "S0510"]
    real = json.loads(Path("config/airports.json").read_text(encoding="utf-8"))["airports"]
    assert required_segments({a["icao"]: a for a in real}) == ["S0410", "S0510"]


# ------------------------------------------------------------------ partition function


def test_partition_requests_both_segments_with_backfill_flag_and_pins(tmp_path):
    pipeline = FakePipeline(tmp_path)
    slots = ["2026-11-15T06:00:00Z", "2026-11-15T06:10:00Z"]
    pins = ["2026-11-09T07:00:00Z"]

    results = list(process_slot_partition(
        slots, AIRPORTS_CFG, str(tmp_path), pins,
        slot_runner=pipeline, archive_checker=_all_available,
    ))

    assert [c["slot"] for c in pipeline.calls] == slots
    assert all(c["segments"] == ["S0410", "S0510"] for c in pipeline.calls)
    assert all(c["is_backfill"] is True and c["pinned_slots"] == pins for c in pipeline.calls)
    assert [r["status"] for r in results] == ["SUCCESS", "SUCCESS"]
    assert results[0]["segments_downloaded"] == ["S0410", "S0510"]
    assert results[0]["bytes_downloaded"] == 200
    assert results[0]["metrics_statuses"] == {"AVAILABLE": 3}
    assert results[0]["seconds"] >= 0


def test_partition_cleans_raw_files_except_pinned_slots(tmp_path):
    pipeline = FakePipeline(tmp_path)
    normal, pinned = "2026-11-15T06:00:00Z", "2026-11-09T07:00:00Z"

    results = list(process_slot_partition(
        [normal, pinned], AIRPORTS_CFG, str(tmp_path), [pinned],
        slot_runner=pipeline, archive_checker=_all_available,
    ))

    assert not _slot_dir(tmp_path, normal).exists()
    assert len(list(_slot_dir(tmp_path, pinned).glob("*.DAT.bz2"))) == 2
    assert results[0]["raw_files_deleted"] == 2 and results[1]["raw_files_deleted"] == 0


def test_partition_survives_a_failing_slot_and_still_cleans_up(tmp_path):
    bad, good = "2026-11-15T06:00:00Z", "2026-11-15T06:10:00Z"
    pipeline = FakePipeline(tmp_path, fail_slots=[bad])

    results = list(process_slot_partition(
        [bad, good], AIRPORTS_CFG, str(tmp_path), [],
        slot_runner=pipeline, archive_checker=_all_available,
    ))

    assert results[0]["status"] == "FAILED" and "satpy exploded" in results[0]["error"]
    assert results[1]["status"] == "SUCCESS"


def test_partition_marks_missing_archive_without_calling_pipeline(tmp_path):
    pipeline = FakePipeline(tmp_path)

    def one_segment_missing(slot_dt, segments):
        return {seg: seg != "S0510" for seg in segments}

    results = list(process_slot_partition(
        ["2026-11-15T06:00:00Z"], AIRPORTS_CFG, str(tmp_path), [],
        slot_runner=pipeline, archive_checker=one_segment_missing,
    ))

    assert pipeline.calls == []
    assert results[0]["status"] == "ARCHIVE_UNAVAILABLE" and "S0510" in results[0]["error"]


def test_partition_flags_substituted_or_incomplete_downloads(tmp_path):
    slot = "2026-11-15T06:00:00Z"

    def substituted(**kwargs):
        res = FakePipeline(tmp_path)(**kwargs)
        res["download"]["source_slot_utc"] = "2026-11-16T06:00:00Z"
        return res

    def single_segment(**kwargs):
        res = FakePipeline(tmp_path)(**kwargs)
        res["download"]["files"].pop("S0510")
        return res

    sub = list(process_slot_partition([slot], AIRPORTS_CFG, str(tmp_path), [],
                                      slot_runner=substituted, archive_checker=_all_available))
    inc = list(process_slot_partition([slot], AIRPORTS_CFG, str(tmp_path), [],
                                      slot_runner=single_segment, archive_checker=_all_available))
    assert sub[0]["status"] == "FALLBACK_SUBSTITUTED"
    assert inc[0]["status"] == "INCOMPLETE_SEGMENTS"


def test_partition_appends_per_slot_rows_to_backfill_log(tmp_path):
    log_path = tmp_path / "logs" / "backfill_log.jsonl"
    list(process_slot_partition(
        ["2026-11-15T06:00:00Z", "2026-11-15T06:10:00Z"], AIRPORTS_CFG, str(tmp_path), [],
        log_path=str(log_path), slot_runner=FakePipeline(tmp_path), archive_checker=_all_available,
    ))
    rows = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    assert [r["slot_utc"] for r in rows] == ["2026-11-15T06:00:00Z", "2026-11-15T06:10:00Z"]
    assert all(r["event"] == "slot" and "seconds" in r and "bytes_downloaded" in r for r in rows)


def test_check_slot_archive_requires_http_200_for_every_segment():
    seen = []

    class Resp:
        def __init__(self, code):
            self.status_code = code

    def head(url, timeout):
        seen.append(url)
        return Resp(200 if "S0410" in url else 404)

    avail = hb.check_slot_archive(datetime(2026, 11, 15, 6, 0, tzinfo=timezone.utc), ["S0410", "S0510"], head=head)
    assert avail == {"S0410": True, "S0510": False}
    assert any("S0510" in u for u in seen)


def test_summarize_slot_results_reports_throughput_and_totals():
    results = [
        {"status": "SUCCESS", "seconds": 10.0, "bytes_downloaded": 100, "raw_files_deleted": 2, "peak_rss_mb": 500.0},
        {"status": "SUCCESS", "seconds": 20.0, "bytes_downloaded": 300, "raw_files_deleted": 2, "peak_rss_mb": 700.0},
        {"status": "FAILED", "seconds": 30.0, "bytes_downloaded": 0, "raw_files_deleted": 0, "peak_rss_mb": None},
    ]
    summary = summarize_slot_results(results, wall_seconds=60.0, skipped_existing=4)
    assert summary["slots_attempted"] == 3 and summary["slots_skipped_existing"] == 4
    assert summary["status_counts"] == {"SUCCESS": 2, "FAILED": 1}
    assert summary["slots_per_min"] == 3.0
    assert summary["bytes_downloaded"] == 400
    assert summary["median_slot_seconds"] == 20.0 and summary["peak_worker_rss_mb"] == 700.0


def test_parallelize_receives_slots_and_parallelism_without_real_spark(tmp_path, mocker):
    """The driver hands the listed slots to sparkContext.parallelize(slots, parallelism).mapPartitions(...)."""
    captured = {}

    class FakeRDD:
        def __init__(self, slots):
            self.slots = slots

        def mapPartitions(self, fn):
            captured["fn"] = fn
            return self

        def collect(self):
            return list(captured["fn"](iter(self.slots)))

    class FakeContext:
        def parallelize(self, data, numSlices):
            captured["slots"], captured["parallelism"] = list(data), numSlices
            return FakeRDD(list(data))

    class FakeSpark:
        sparkContext = FakeContext()

    pipeline = FakePipeline(tmp_path)
    mocker.patch("src.spark.himawari_batch._default_slot_runner", return_value=pipeline)
    mocker.patch("src.spark.himawari_batch.check_slot_archive", side_effect=_all_available)
    mocker.patch("src.ingestion.config_loader.load_airports_config", return_value=AIRPORTS_CFG)

    outcome = hb.run_distributed_himawari_backfill(
        "2026-11-15T06:00:00Z", "2026-11-15T06:50:00Z", parallelism=3,
        base_dir=str(tmp_path), spark=FakeSpark(),
    )

    assert captured["parallelism"] == 3
    assert len(captured["slots"]) == 6
    assert outcome["summary"]["slots_attempted"] == 6 and outcome["summary"]["parallelism"] == 3
    assert outcome["summary"]["segments"] == ["S0410", "S0510"]
    assert all(c["segments"] == ["S0410", "S0510"] for c in pipeline.calls)
    log_lines = (tmp_path / "logs" / "backfill_log.jsonl").read_text(encoding="utf-8").splitlines()
    assert json.loads(log_lines[-1])["event"] == "summary" and len(log_lines) == 7
