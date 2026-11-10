"""Unit and integration tests for storage migration, verification, backup, and pruning."""

import collections
import hashlib
import json
from pathlib import Path
import zipfile
import pytest

from src.ingestion.raw_store import RawEnvelopeStore
from src.ingestion.migrate_storage import (
    compute_canonical_hash,
    migrate_storage,
    prune_legacy_storage,
    rollback_storage,
)


def _setup_legacy_sample_data(tmp_path: Path) -> list[str]:
    """Populates legacy JSON files across metar and open_meteo for testing."""
    root = tmp_path / "data"
    raw_dir = root / "raw"

    metar_dir = raw_dir / "metar" / "date=2026-11-09"
    metar_dir.mkdir(parents=True, exist_ok=True)
    om_dir = raw_dir / "open_meteo" / "date=2026-11-09"
    om_dir.mkdir(parents=True, exist_ok=True)

    # Also add a raw binary satellite file that MUST NEVER be pruned
    hsd_dir = raw_dir / "himawari_hsd" / "date=2026-11-09"
    hsd_dir.mkdir(parents=True, exist_ok=True)
    (hsd_dir / "HS_H09_20261109_0800_B13_FLDK_R20_S0410.DAT.bz2").write_bytes(b"SATELLITE_BINARY_DATA")

    created_files = []
    # 2 METAR files
    for i in (1, 2):
        env = {
            "metadata": {
                "batch_id": f"batch_metar_{i}",
                "source": "aviationweather_metar",
                "expected_slot_utc": f"2026-11-09T0{i}:00:00Z",
                "fetched_at_utc": f"2026-11-09T0{i}:00:00Z",
            },
            "payload": [{"icaoId": "VVDN", "rawOb": f"METAR VVDN {i}00Z"}],
        }
        f_path = metar_dir / f"metar_batch_{i}.json"
        f_path.write_text(json.dumps(env), encoding="utf-8")
        created_files.append(str(f_path))

    # 1 duplicate METAR file (identical content, different file name)
    dup_env = {
        "metadata": {
            "batch_id": "batch_metar_dup",
            "source": "aviationweather_metar",
            "expected_slot_utc": "2026-11-09T03:00:00Z",
            "fetched_at_utc": "2026-11-09T03:00:00Z",
        },
        "payload": [{"icaoId": "VVNB", "rawOb": "METAR VVNB 0300Z"}],
    }
    f_dup1 = metar_dir / "metar_batch_dup1.json"
    f_dup2 = metar_dir / "metar_batch_dup2.json"
    f_dup1.write_text(json.dumps(dup_env), encoding="utf-8")
    f_dup2.write_text(json.dumps(dup_env), encoding="utf-8")
    created_files.extend([str(f_dup1), str(f_dup2)])

    # 1 Open-Meteo file
    om_env = {
        "metadata": {
            "batch_id": "batch_om_1",
            "source": "open_meteo_forecast",
            "airport_icao": "VVDN",
            "expected_slot_utc": "2026-11-09T01:00:00Z",
            "fetched_at_utc": "2026-11-09T01:00:00Z",
        },
        "payload": {"hourly": {"time": ["2026-11-09T02:00"], "precipitation": [0.0]}},
    }
    f_om = om_dir / "open_meteo_VVDN_batch_1.json"
    f_om.write_text(json.dumps(om_env), encoding="utf-8")
    created_files.append(str(f_om))

    return created_files


def _snapshot_file_tree(root: Path) -> dict[str, str]:
    """Returns mapping of relative path to SHA-256 for all files in root."""
    res = {}
    if not root.exists():
        return res
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            res[rel] = hashlib.sha256(p.read_bytes()).hexdigest()
    return res


def test_migration_dry_run_is_strictly_read_only(tmp_path: Path):
    """TC-MIGR-01: dry-run must not modify any files and report expected counts."""
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)

    tree_before = _snapshot_file_tree(root)

    report = migrate_storage(str(root), action="dry-run")

    tree_after = _snapshot_file_tree(root)
    assert tree_before == tree_after
    assert report["status"] == "DRY_RUN_SUCCESS"
    assert report["source_files_count"] == 5
    assert report["corrupt_files_count"] == 0
    assert report["active_layout"] == "legacy_files"


def test_migration_preserves_multiset_canonical_parity(tmp_path: Path):
    """TC-MIGR-02: apply must preserve all batches and match multiset canonical hash."""
    root = tmp_path / "data"
    created = _setup_legacy_sample_data(tmp_path)

    # Compute expected multiset of canonical hashes from legacy files
    expected_hashes = collections.Counter()
    for fp in created:
        env = json.loads(Path(fp).read_text(encoding="utf-8"))
        expected_hashes[compute_canonical_hash(env)] += 1

    report = migrate_storage(str(root), action="apply")
    assert report["status"] == "SUCCESS"
    assert report["migrated_files_count"] == 5
    assert report["active_layout"] == "daily_jsonl"

    # Verify store active layout marker
    store = RawEnvelopeStore(str(root))
    assert store.active_layout() == "daily_jsonl"

    # Verify multiset parity from compacted envelopes
    actual_hashes = collections.Counter()
    for source in ("metar", "open_meteo"):
        for env in store.iter_envelopes(source):
            actual_hashes[compute_canonical_hash(env)] += 1

    assert actual_hashes == expected_hashes


def test_migration_halts_on_corrupt_legacy_file(tmp_path: Path):
    """Corrupt JSON in legacy directory must abort migration before any changes."""
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)

    corrupt_file = root / "raw" / "metar" / "date=2026-11-09" / "corrupt_batch.json"
    corrupt_file.write_text("NOT A VALID JSON {{{", encoding="utf-8")

    tree_before = _snapshot_file_tree(root)

    report = migrate_storage(str(root), action="apply")
    assert report["status"] == "ABORTED_CORRUPT_FILES"
    assert len(report["corrupt_files"]) == 1
    assert "corrupt_batch.json" in report["corrupt_files"][0]

    tree_after = _snapshot_file_tree(root)
    # Ensure no raw_compact files were published
    assert not (root / "raw_compact").exists()


def test_prune_legacy_requires_valid_zip_backup(tmp_path: Path):
    """TC-MIGR-03: prune creates verified zip, deletes legacy json, preserves HSD binary."""
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)

    # 1. Apply migration
    migrate_storage(str(root), action="apply")

    # 2. Prune legacy
    hsd_file = root / "raw" / "himawari_hsd" / "date=2026-11-09" / "HS_H09_20261109_0800_B13_FLDK_R20_S0410.DAT.bz2"
    assert hsd_file.exists()

    prune_res = prune_legacy_storage(str(root))
    assert prune_res["status"] == "PRUNE_SUCCESS"
    assert prune_res["files_deleted_count"] == 5

    # Check that backup ZIP exists and has all 5 files
    zip_path = root / "backups" / "raw-json-before-migration.zip"
    assert zip_path.exists()

    with zipfile.ZipFile(zip_path, "r") as zf:
        namelist = zf.namelist()
        assert len(namelist) == 5
        for name in namelist:
            assert name.endswith(".json")

    # Satellite HSD binary MUST still exist!
    assert hsd_file.exists()

    # Legacy JSON files must be gone
    metar_jsons = list((root / "raw" / "metar").glob("date=*/*.json"))
    assert len(metar_jsons) == 0


def _new_metar_env(batch_id: str, slot_hour: int = 4) -> dict:
    return {
        "metadata": {
            "batch_id": batch_id,
            "source": "aviationweather_metar",
            "expected_slot_utc": f"2026-11-09T0{slot_hour}:00:00Z",
            "fetched_at_utc": f"2026-11-09T0{slot_hour}:00:00Z",
        },
        "payload": [{"icaoId": "VVTS", "rawOb": f"METAR VVTS 0{slot_hour}00Z"}],
    }


def test_apply_twice_is_noop_and_keeps_new_batches(tmp_path: Path):
    """Re-running apply after activation must not overwrite batches written since cutover."""
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)

    assert migrate_storage(str(root), action="apply")["status"] == "SUCCESS"

    store = RawEnvelopeStore(str(root))
    store.write("metar", "2026-11-09", _new_metar_env("batch_after_cutover"))
    shard = root / "raw_compact" / "metar" / "date=2026-11-09" / "batches-0001.jsonl"
    before = shard.read_bytes()

    second = migrate_storage(str(root), action="apply")
    assert second["status"] == "ALREADY_ACTIVE"
    assert shard.read_bytes() == before
    ids = [e["metadata"]["batch_id"] for e in store.iter_envelopes("metar")]
    assert "batch_after_cutover" in ids


def test_apply_shards_by_shard_max_bytes(tmp_path: Path):
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)

    report = migrate_storage(str(root), action="apply", shard_max_bytes=250)
    assert report["status"] == "SUCCESS"
    shards = sorted((root / "raw_compact" / "metar" / "date=2026-11-09").glob("batches-*.jsonl"))
    assert len(shards) > 1
    assert all(s.stat().st_size <= 250 or len(s.read_text(encoding="utf-8").splitlines()) == 1 for s in shards)
    assert len(list(RawEnvelopeStore(str(root)).iter_envelopes("metar"))) == 4


def test_apply_verifies_staged_shards_read_back_from_disk(tmp_path: Path, monkeypatch):
    """A corrupted staged shard must abort and leave legacy data and layout untouched."""
    import src.ingestion.migrate_storage as ms

    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)

    real_write = ms._write_partition_shards

    def corrupting_write(target_dir, files, shard_max_bytes):
        n = real_write(target_dir, files, shard_max_bytes)
        first = sorted(target_dir.glob("batches-*.jsonl"))[0]
        first.write_bytes(first.read_bytes().replace(b"VVDN", b"XXXX"))
        return n

    monkeypatch.setattr(ms, "_write_partition_shards", corrupting_write)
    res = migrate_storage(str(root), action="apply")

    assert res["status"] == "ABORTED_CHECKSUM_MISMATCH"
    assert not (root / "raw_compact").exists()
    assert not (root / "raw_compact.staging").exists()
    assert not (root / "storage_state.json").exists()
    assert len(list((root / "raw" / "metar").glob("date=*/*.json"))) == 4


def test_apply_aborts_on_same_batch_id_with_different_content(tmp_path: Path):
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)
    metar_dir = root / "raw" / "metar" / "date=2026-11-09"
    conflict = json.loads((metar_dir / "metar_batch_1.json").read_text(encoding="utf-8"))
    conflict["payload"][0]["rawOb"] = "CHANGED"
    (metar_dir / "metar_batch_1_conflict.json").write_text(json.dumps(conflict), encoding="utf-8")

    res = migrate_storage(str(root), action="apply")
    assert res["status"] == "ABORTED_BATCH_CONFLICT"
    assert not (root / "raw_compact").exists()


def test_prune_skips_files_changed_after_migration(tmp_path: Path):
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)
    migrate_storage(str(root), action="apply")

    changed = root / "raw" / "metar" / "date=2026-11-09" / "metar_batch_1.json"
    changed.write_text('{"edited": true}', encoding="utf-8")

    res = prune_legacy_storage(str(root))
    assert res["status"] == "PRUNE_SUCCESS"
    assert res["files_deleted_count"] == 4
    assert changed.exists()
    assert any(s["rel_path"].endswith("metar_batch_1.json") and s["reason"] == "changed_since_migration"
               for s in res["skipped"])


def test_prune_rejects_path_traversal_entries(tmp_path: Path):
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)
    migrate_storage(str(root), action="apply")

    victim = tmp_path / "victim.json"
    victim.write_text("{}", encoding="utf-8")
    report_path = sorted((root / "logs").glob("migration_report_*.json"))[-1]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["migrated_files"].append({
        "rel_path": "raw/../../victim.json",
        "sha256": hashlib.sha256(b"{}").hexdigest(),
    })
    report_path.write_text(json.dumps(report), encoding="utf-8")

    res = prune_legacy_storage(str(root))
    assert victim.exists()
    assert any(s["reason"] == "invalid_path" for s in res["skipped"])


def test_prune_twice_is_noop(tmp_path: Path):
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)
    migrate_storage(str(root), action="apply")
    assert prune_legacy_storage(str(root))["status"] == "PRUNE_SUCCESS"
    zip_path = root / "backups" / "raw-json-before-migration.zip"
    size = zip_path.stat().st_size

    assert prune_legacy_storage(str(root))["status"] == "NOOP"
    assert zip_path.stat().st_size == size


def test_rollback_twice_does_not_duplicate_files(tmp_path: Path):
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)
    migrate_storage(str(root), action="apply")
    store = RawEnvelopeStore(str(root))
    store.write("metar", "2026-11-09", _new_metar_env("batch_new"))

    first = rollback_storage(str(root))
    count_after_first = len(list((root / "raw").rglob("*.json")))
    second = rollback_storage(str(root))
    assert first["new_batches_extracted_to_legacy"] == 1
    assert second["new_batches_extracted_to_legacy"] == 0
    assert len(list((root / "raw").rglob("*.json"))) == count_after_first


def test_rollback_after_prune_restores_all_batches(tmp_path: Path):
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)
    migrate_storage(str(root), action="apply")
    prune_legacy_storage(str(root))
    assert not list((root / "raw" / "metar").glob("date=*/*.json"))

    res = rollback_storage(str(root))
    assert res["status"] == "ROLLBACK_SUCCESS"
    restored = list((root / "raw").glob("*/date=*/*.json"))
    assert len(restored) >= 4  # 3 unique METAR (dup collapsed by content) + 1 open_meteo
    assert RawEnvelopeStore(str(root)).active_layout() == "legacy_files"


def test_reapply_after_rollback_keeps_replaced_compact(tmp_path: Path):
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)
    migrate_storage(str(root), action="apply")
    rollback_storage(str(root))

    res = migrate_storage(str(root), action="apply")
    assert res["status"] == "SUCCESS"
    assert (root / "raw_compact").exists()
    assert len(list(root.glob("raw_compact.replaced-*"))) == 1


def test_verify_detects_missing_batches(tmp_path: Path):
    from src.ingestion.migrate_storage import verify_storage

    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)
    migrate_storage(str(root), action="apply")
    assert verify_storage(str(root))["status"] == "VERIFY_SUCCESS"

    shard = root / "raw_compact" / "metar" / "date=2026-11-09" / "batches-0001.jsonl"
    lines = shard.read_text(encoding="utf-8").splitlines()
    shard.write_text("\n".join(lines[1:]) + "\n", encoding="utf-8")
    assert verify_storage(str(root))["status"] == "VERIFY_MISMATCH"


def test_rollback_preserves_new_batches_written_after_cutover(tmp_path: Path):
    """Rollback restores legacy layout and writes new batches from JSONL into individual JSONs."""
    root = tmp_path / "data"
    _setup_legacy_sample_data(tmp_path)

    # 1. Apply migration to daily_jsonl
    migrate_storage(str(root), action="apply")

    # 2. Ingest a brand new batch into compact layout
    store = RawEnvelopeStore(str(root))
    new_env = {
        "metadata": {
            "batch_id": "batch_new_after_cutover",
            "source": "aviationweather_metar",
            "expected_slot_utc": "2026-11-09T04:00:00Z",
            "fetched_at_utc": "2026-11-09T04:00:00Z",
        },
        "payload": [{"icaoId": "VVTS", "rawOb": "METAR VVTS 0400Z"}],
    }
    store.write("metar", "2026-11-09", new_env)

    # 3. Rollback
    rb_res = rollback_storage(str(root))
    assert rb_res["status"] == "ROLLBACK_SUCCESS"
    assert store.active_layout() == "legacy_files"

    # Verify that the new batch was extracted to legacy json!
    new_json = root / "raw" / "metar" / "date=2026-11-09" / "metar_batch_new_after_cutover.json"
    assert new_json.exists()
    content = json.loads(new_json.read_text(encoding="utf-8"))
    assert content["metadata"]["batch_id"] == "batch_new_after_cutover"

