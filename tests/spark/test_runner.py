"""Long-lived runner: re-runs only when Bronze changes, survives pipeline errors, reports a heartbeat."""

from pathlib import Path

from src.ingestion.raw_store import RawEnvelopeStore
from src.spark import run_pipeline, runner


def _write_metar(root: Path, batch: str) -> None:
    RawEnvelopeStore(storage_root=str(root)).write("metar", "2026-11-10", {
        "metadata": {"batch_id": batch, "source": "aviationweather_metar", "fetched_at_utc": "2026-11-10T08:01:00Z"},
        "payload": [],
    })


def test_runner_reruns_on_bronze_change_and_survives_errors(spark, tmp_path: Path, monkeypatch):
    root = tmp_path / "data"
    _write_metar(root, "b1")
    calls = []

    def fake_pipeline(**kwargs):
        calls.append(kwargs["storage_root"])
        if len(calls) == 2:
            raise RuntimeError("JVM hiccup")
        return {"run_id": f"r{len(calls)}"}

    monkeypatch.setattr(run_pipeline, "run_spark_pipeline", fake_pipeline)
    cleared = []
    monkeypatch.setattr(spark.catalog, "clearCache", lambda: cleared.append(True))

    def sleep(_seconds):
        n = sleep.n = getattr(sleep, "n", 0) + 1
        if n in (2, 3):  # ingestion writes between polls 2->3 and 3->4; poll 1->2 sees no change
            _write_metar(root, f"b{n}")
        if n == 3:
            assert runner.read_heartbeat(str(root))["status"] == "ERROR"

    runs = runner.run_forever(storage_root=str(root), poll_seconds=0, max_cycles=5, sleep=sleep)

    # cycle 1 runs (first poll), 2 skips (unchanged), 3 runs and fails, 4 runs, 5 skips
    assert runs == 3 and len(calls) == 3
    assert len(cleared) == 3  # caches cleared after every run, including the failed one
    beat = runner.read_heartbeat(str(root))
    assert beat["status"] == "IDLE" and beat["last_run_id"] == "r3" and beat["last_error"] is None
