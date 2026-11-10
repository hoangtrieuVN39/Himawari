"""Tests for RawEnvelopeStore: daily JSONL compaction, concurrency, and integrity."""

import json
import logging
from multiprocessing import Process, Queue
from pathlib import Path
import pytest

from src.ingestion.raw_store import (
    RawEnvelopeStore,
    WriteReceipt,
    ReadSpec,
    BatchConflictError,
    CorruptStorageStateError,
    CorruptShardError,
    LockTimeoutError,
)


def _make_envelope(source: str, batch_id: str, payload_data: dict, is_backfill: bool = False) -> dict:
    return {
        "metadata": {
            "batch_id": batch_id,
            "source": source,
            "fetched_at_utc": "2026-11-10T08:00:00Z",
            "is_backfill": is_backfill,
        },
        "payload": payload_data,
    }


def test_daily_store_keeps_all_batches_in_three_files(tmp_path: Path):
    store = RawEnvelopeStore(storage_root=str(tmp_path))
    date_today = "2026-11-10"

    # 1. Write 144 METAR batches
    for i in range(144):
        env = _make_envelope("metar", f"metar_{i:03d}", {"obs": f"VVDN 10{i:02d}00Z"})
        receipt = store.write("metar", date_today, env)
        assert receipt.inserted is True
        assert receipt.bytes_added > 0

    # 2. Write 72 Open-Meteo batches (3 airports)
    for i in range(24):
        for ap in ("VVDN", "VVNB", "VVTS"):
            env = _make_envelope("open_meteo", f"fc_{ap}_{i:02d}", {"airport": ap, "rain": 0.5})
            env["metadata"]["airport_icao"] = ap
            receipt = store.write("open_meteo", date_today, env)
            assert receipt.inserted is True

    # 3. Write 144 Himawari metrics batches
    for i in range(144):
        env = _make_envelope("himawari_metrics", f"hima_{i:03d}", {"bt_min": 25.0})
        receipt = store.write("himawari_metrics", date_today, env)
        assert receipt.inserted is True

    # Check files created
    metar_files = list((tmp_path / "raw_compact" / "metar" / f"date={date_today}").glob("*.jsonl"))
    fc_files = list((tmp_path / "raw_compact" / "open_meteo" / f"date={date_today}").glob("*.jsonl"))
    hima_files = list((tmp_path / "raw_compact" / "himawari_metrics" / f"date={date_today}").glob("*.jsonl"))

    assert len(metar_files) == 1
    assert metar_files[0].name == "batches-0001.jsonl"
    assert len(fc_files) == 1
    assert fc_files[0].name == "batches-0001.jsonl"
    assert len(hima_files) == 1
    assert hima_files[0].name == "batches-0001.jsonl"

    # Read back all 360 batches
    read_metar = list(store.iter_envelopes("metar", date_str=date_today))
    read_fc = list(store.iter_envelopes("open_meteo", date_str=date_today))
    read_hima = list(store.iter_envelopes("himawari_metrics", date_str=date_today))

    assert len(read_metar) == 144
    assert len(read_fc) == 72
    assert len(read_hima) == 144

    # Next day creates separate partition directory
    date_tomorrow = "2026-11-11"
    store.write("metar", date_tomorrow, _make_envelope("metar", "metar_next_01", {"obs": "next day"}))
    next_files = list((tmp_path / "raw_compact" / "metar" / f"date={date_tomorrow}").glob("*.jsonl"))
    assert len(next_files) == 1


def test_shard_rollover_at_threshold(tmp_path: Path):
    # Set threshold: 700 bytes (each envelope with 200 'x' is ~341 bytes)
    # r1: 341B <= 700B -> shard 1
    # r2: 341B + 341B = 682B <= 700B -> shard 1
    # r3: 682B + 341B = 1023B > 700B -> shard 2
    store = RawEnvelopeStore(storage_root=str(tmp_path), shard_max_bytes=700)
    date_str = "2026-11-10"

    # Write multiple envelopes until rollover
    r1 = store.write("metar", date_str, _make_envelope("metar", "b1", {"data": "x" * 200}))
    r2 = store.write("metar", date_str, _make_envelope("metar", "b2", {"data": "x" * 200}))
    r3 = store.write("metar", date_str, _make_envelope("metar", "b3", {"data": "x" * 200}))

    assert r1.shard_index == 1
    assert r2.shard_index == 1
    assert r3.shard_index == 2
    assert "batches-0002.jsonl" in r3.path

    # Verify both shards can be read back in order
    all_envs = list(store.iter_envelopes("metar", date_str=date_str))
    assert len(all_envs) == 3
    assert [e["metadata"]["batch_id"] for e in all_envs] == ["b1", "b2", "b3"]


def test_idempotency_retry_and_conflict(tmp_path: Path):
    store = RawEnvelopeStore(storage_root=str(tmp_path))
    date_str = "2026-11-10"

    env = _make_envelope("metar", "batch_repeat", {"temp": 28.0})
    r1 = store.write("metar", date_str, env)
    assert r1.inserted is True
    assert r1.bytes_added > 0

    # Retry same batch_id with same content is a no-op
    r2 = store.write("metar", date_str, env)
    assert r2.inserted is False
    assert r2.bytes_added == 0

    # Trùng batch_id khác payload -> BatchConflictError
    conflicting_env = _make_envelope("metar", "batch_repeat", {"temp": 99.0})
    with pytest.raises(BatchConflictError):
        store.write("metar", date_str, conflicting_env)

    # Khác batch_id cùng payload -> cả 2 được lưu
    same_payload_diff_id = _make_envelope("metar", "batch_different_id", {"temp": 28.0})
    r3 = store.write("metar", date_str, same_payload_diff_id)
    assert r3.inserted is True
    assert len(list(store.iter_envelopes("metar", date_str=date_str))) == 2


def test_storage_state_discovery(tmp_path: Path):
    # 1. Fresh directory -> auto daily_jsonl
    store = RawEnvelopeStore(storage_root=str(tmp_path))
    assert store.active_layout() == "daily_jsonl"
    state_file = tmp_path / "storage_state.json"
    assert state_file.exists()
    state_data = json.loads(state_file.read_text(encoding="utf-8"))
    assert state_data["raw_layout"] == "daily_jsonl"

    # 2. Existing legacy without marker -> selects legacy_files
    legacy_root = tmp_path / "legacy_test"
    legacy_metar = legacy_root / "raw" / "metar" / "date=2026-11-09"
    legacy_metar.mkdir(parents=True, exist_ok=True)
    (legacy_metar / "sample.json").write_text("{}", encoding="utf-8")

    legacy_store = RawEnvelopeStore(storage_root=str(legacy_root))
    assert legacy_store.active_layout() == "legacy_files"

    # 3. Corrupt marker -> CorruptStorageStateError
    corrupt_root = tmp_path / "corrupt_test"
    corrupt_root.mkdir(parents=True, exist_ok=True)
    (corrupt_root / "storage_state.json").write_text("{malformed json", encoding="utf-8")
    with pytest.raises(CorruptStorageStateError):
        RawEnvelopeStore(storage_root=str(corrupt_root)).active_layout()


def test_corrupt_line_handling_in_iter_envelopes(tmp_path: Path, caplog):
    store = RawEnvelopeStore(storage_root=str(tmp_path))
    date_str = "2026-11-10"

    store.write("metar", date_str, _make_envelope("metar", "b1", {"line": 1}))
    store.write("metar", date_str, _make_envelope("metar", "b2", {"line": 2}))

    shard_path = tmp_path / "raw_compact" / "metar" / f"date={date_str}" / "batches-0001.jsonl"
    # Inject corrupt line in between
    lines = shard_path.read_text(encoding="utf-8").splitlines()
    corrupted = lines[0] + "\n{CORRUPT_NOT_JSON}\n" + lines[1] + "\n"
    shard_path.write_text(corrupted, encoding="utf-8")

    # strict=True raises CorruptShardError
    with pytest.raises(CorruptShardError):
        list(store.iter_envelopes("metar", date_str=date_str, strict=True))

    # strict=False skips bad line with warning log
    with caplog.at_level(logging.WARNING):
        recovered = list(store.iter_envelopes("metar", date_str=date_str, strict=False))
        assert len(recovered) == 2
        assert [e["metadata"]["batch_id"] for e in recovered] == ["b1", "b2"]
        assert any("corrupt line" in rec.message.lower() for rec in caplog.records)


def test_lock_timeout_raises_lock_timeout_error(tmp_path: Path):
    """While one holder owns the lock, a second writer must fail fast with LockTimeoutError."""
    holder = RawEnvelopeStore(storage_root=str(tmp_path), lock_timeout_sec=0.2)
    contender = RawEnvelopeStore(storage_root=str(tmp_path), lock_timeout_sec=0.2)

    with holder.lock():
        with pytest.raises(LockTimeoutError):
            contender.write("metar", "2026-11-10", _make_envelope("metar", "b_locked", {"x": 1}))

    # After release the contender can write normally
    receipt = contender.write("metar", "2026-11-10", _make_envelope("metar", "b_locked", {"x": 1}))
    assert receipt.inserted is True


def test_read_paths_do_not_create_marker(tmp_path: Path):
    """Reading from a fresh root must not mutate storage (no storage_state.json side effect)."""
    store = RawEnvelopeStore(storage_root=str(tmp_path))
    assert list(store.iter_envelopes("metar")) == []
    store.read_specs("metar")
    store.signature()
    assert store.peek_layout() == "daily_jsonl"
    assert not (tmp_path / "storage_state.json").exists()

    # First write initialises the marker under lock
    store.write("metar", "2026-11-10", _make_envelope("metar", "b1", {"x": 1}))
    assert (tmp_path / "storage_state.json").exists()


def _worker_write(storage_root: str, source: str, date_str: str, worker_id: int, count: int, q: Queue):
    try:
        store = RawEnvelopeStore(storage_root=storage_root)
        for i in range(count):
            env = _make_envelope(source, f"worker_{worker_id}_{i:03d}", {"val": i})
            store.write(source, date_str, env)
        q.put(("SUCCESS", worker_id))
    except Exception as exc:
        q.put(("ERROR", worker_id, str(exc)))


def test_concurrent_multiprocess_writers(tmp_path: Path):
    date_str = "2026-11-10"
    num_workers = 3
    batches_per_worker = 15
    q = Queue()

    procs = [
        Process(target=_worker_write, args=(str(tmp_path), "metar", date_str, w_id, batches_per_worker, q))
        for w_id in range(num_workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)

    results = [q.get_nowait() for _ in range(num_workers)]
    for r in results:
        assert r[0] == "SUCCESS", f"Worker failed: {r}"

    store = RawEnvelopeStore(storage_root=str(tmp_path))
    envelopes = list(store.iter_envelopes("metar", date_str=date_str))
    assert len(envelopes) == num_workers * batches_per_worker

