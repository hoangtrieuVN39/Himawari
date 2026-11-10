"""Storage Migration and Physical Compaction CLI and Library.

Safely converts discrete raw JSON envelopes to partitioned Daily JSONL shards.

Safety model:
- Every mutating action runs under the single RawEnvelopeStore lock, scan included.
- ``apply`` stages shards in ``raw_compact.staging``, re-reads them from disk, compares the
  multiset of canonical SHA-256 hashes with the legacy files, and only then swaps the directory
  in and flips the layout marker. A failed check removes the staging directory and nothing else.
- ``apply`` after activation is a no-op (``ALREADY_ACTIVE``).
- ``prune-legacy`` only deletes files whose SHA-256 still equals the one recorded at migration.
- ``rollback`` extracts compact-only batches back into legacy JSON files idempotently.
"""

from __future__ import annotations

import argparse
import collections
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sys
from typing import Any, Dict, Iterator, List, Optional
import zipfile

from src.ingestion.fs_utils import atomic_write_bytes, file_sha256, is_temp_file, safe_replace
from src.ingestion.raw_store import (
    LAYOUT_DAILY_JSONL,
    LAYOUT_LEGACY_FILES,
    SUPPORTED_SOURCES,
    RawEnvelopeStore,
)

logger = logging.getLogger(__name__)

STAGING_DIRNAME = "raw_compact.staging"
COMPACT_DIRNAME = "raw_compact"
REPORT_GLOB = "migration_report_*.json"
BACKUP_ZIP_NAME = "raw-json-before-migration.zip"
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._=-]")


def compute_canonical_hash(envelope: Dict[str, Any]) -> str:
    """Computes SHA-256 over key-sorted, whitespace-stripped UTF-8 canonical JSON."""
    canonical_bytes = json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class LegacyFile:
    source: str
    date: str
    path: Path
    rel_path: str
    canonical_hash: str
    file_sha256: str
    batch_id: Optional[str]


def _scan_legacy(root: Path) -> tuple[list[LegacyFile], list[str], int]:
    """Scans raw/<source>/date=*/*.json. Returns (files, corrupt descriptions, total file count)."""
    files: list[LegacyFile] = []
    corrupt: list[str] = []
    total = 0
    raw_dir = root / "raw"
    if not raw_dir.exists():
        return files, corrupt, total

    for source in SUPPORTED_SOURCES:
        s_dir = raw_dir / source
        if not s_dir.exists():
            continue
        for date_dir in sorted(s_dir.glob("date=*")):
            if not date_dir.is_dir():
                continue
            date_str = date_dir.name.split("=", 1)[1]
            for f_path in sorted(date_dir.glob("*.json")):
                if is_temp_file(f_path.name):
                    continue
                total += 1
                try:
                    raw_bytes = f_path.read_bytes()
                    env = json.loads(raw_bytes.decode("utf-8"))
                    files.append(
                        LegacyFile(
                            source=source,
                            date=date_str,
                            path=f_path,
                            rel_path=f_path.relative_to(root).as_posix(),
                            canonical_hash=compute_canonical_hash(env),
                            file_sha256=hashlib.sha256(raw_bytes).hexdigest(),
                            batch_id=(env.get("metadata") or {}).get("batch_id") if isinstance(env, dict) else None,
                        )
                    )
                except Exception as err:
                    corrupt.append(f"{f_path.as_posix()}: {err}")
    return files, corrupt, total


def _find_batch_conflicts(files: list[LegacyFile]) -> list[str]:
    """Same (source, batch_id) with different canonical content."""
    seen: dict[tuple[str, str], set[str]] = collections.defaultdict(set)
    for lf in files:
        if lf.batch_id:
            seen[(lf.source, lf.batch_id)].add(lf.canonical_hash)
    return [f"{src}:{bid}" for (src, bid), hashes in sorted(seen.items()) if len(hashes) > 1]


def _write_partition_shards(target_dir: Path, files: list[Path], shard_max_bytes: int) -> int:
    """Streams legacy files into batches-NNNN.jsonl shards (roll over at shard_max_bytes). Returns shard count."""
    target_dir.mkdir(parents=True, exist_ok=True)
    idx = 0
    cur_size = 0
    fh = None
    try:
        for fp in files:
            env = json.loads(fp.read_text(encoding="utf-8"))
            line = json.dumps(env, ensure_ascii=False).encode("utf-8") + b"\n"
            if fh is None or (cur_size > 0 and cur_size + len(line) > shard_max_bytes):
                if fh is not None:
                    fh.flush()
                    os.fsync(fh.fileno())
                    fh.close()
                idx += 1
                fh = open(target_dir / f"batches-{idx:04d}.jsonl", "wb")
                cur_size = 0
            fh.write(line)
            cur_size += len(line)
    finally:
        if fh is not None:
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()
    return idx


def _iter_shard_envelopes(compact_dir: Path) -> Iterator[Dict[str, Any]]:
    for source in SUPPORTED_SOURCES:
        s_dir = compact_dir / source
        if not s_dir.exists():
            continue
        for shard in sorted(s_dir.glob("date=*/batches-*.jsonl")):
            if is_temp_file(shard.name):
                continue
            with open(shard, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        yield json.loads(line)


def _latest_report(root: Path) -> Optional[Path]:
    reports = sorted((root / "logs").glob(REPORT_GLOB)) if (root / "logs").exists() else []
    return reports[-1] if reports else None


def migrate_storage(
    storage_root: str = "data",
    *,
    action: str = "dry-run",
    shard_max_bytes: int = 64 * 1024 * 1024,
) -> Dict[str, Any]:
    """Performs dry-run or applies storage migration from legacy files to daily JSONL."""
    if action not in ("dry-run", "apply"):
        raise ValueError(f"Unknown migration action: '{action}'. Expected 'dry-run' or 'apply'.")

    root = Path(storage_root)
    store = RawEnvelopeStore(storage_root=storage_root, shard_max_bytes=shard_max_bytes)

    if action == "dry-run":
        files, corrupt, total = _scan_legacy(root)
        if corrupt:
            return {
                "status": "ABORTED_CORRUPT_FILES",
                "storage_root": str(root),
                "corrupt_files_count": len(corrupt),
                "corrupt_files": corrupt,
                "source_files_count": total,
                "active_layout": store.peek_layout(),
            }
        return {
            "status": "DRY_RUN_SUCCESS",
            "storage_root": str(root),
            "source_files_count": total,
            "target_files_count": len({(f.source, f.date) for f in files}),
            "corrupt_files_count": 0,
            "active_layout": store.peek_layout(),
        }

    with store.lock():
        if store.state_file.exists() and store.peek_layout() == LAYOUT_DAILY_JSONL:
            return {
                "status": "ALREADY_ACTIVE",
                "storage_root": str(root),
                "active_layout": LAYOUT_DAILY_JSONL,
            }

        files, corrupt, total = _scan_legacy(root)
        if corrupt:
            return {
                "status": "ABORTED_CORRUPT_FILES",
                "storage_root": str(root),
                "corrupt_files_count": len(corrupt),
                "corrupt_files": corrupt,
                "source_files_count": total,
                "active_layout": store.peek_layout(),
            }

        conflicts = _find_batch_conflicts(files)
        if conflicts:
            return {
                "status": "ABORTED_BATCH_CONFLICT",
                "storage_root": str(root),
                "conflicting_batches": conflicts,
                "active_layout": store.peek_layout(),
            }

        legacy_hashes = collections.Counter(f.canonical_hash for f in files)
        grouped: dict[tuple[str, str], list[Path]] = collections.defaultdict(list)
        for lf in files:
            grouped[(lf.source, lf.date)].append(lf.path)

        staging = root / STAGING_DIRNAME
        compact_dir = root / COMPACT_DIRNAME
        if staging.exists():
            shutil.rmtree(staging)  # stale staging from a crashed run; never published

        try:
            shard_count = 0
            for (src, dt), paths in sorted(grouped.items()):
                shard_count += _write_partition_shards(staging / src / f"date={dt}", paths, shard_max_bytes)

            # Verify by re-reading what is actually on disk.
            staged_hashes = collections.Counter(
                compute_canonical_hash(env) for env in _iter_shard_envelopes(staging)
            )
            if staged_hashes != legacy_hashes:
                shutil.rmtree(staging, ignore_errors=True)
                return {
                    "status": "ABORTED_CHECKSUM_MISMATCH",
                    "storage_root": str(root),
                    "legacy_envelopes": sum(legacy_hashes.values()),
                    "staged_envelopes": sum(staged_hashes.values()),
                }
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise

        # Publish: keep any previous raw_compact aside (e.g. left over from an earlier rollback).
        replaced_dir: Optional[Path] = None
        if compact_dir.exists():
            replaced_dir = root / f"{COMPACT_DIRNAME}.replaced-{_now_stamp()}"
            safe_replace(compact_dir, replaced_dir)
        safe_replace(staging, compact_dir)
        store.set_layout(LAYOUT_DAILY_JSONL)

        stamp = _now_stamp()
        report_path = root / "logs" / f"migration_report_{stamp}.json"
        report_data = {
            "status": "SUCCESS",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "storage_root": str(root),
            "migrated_files_count": len(files),
            "migrated_files": [
                {
                    "rel_path": f.rel_path,
                    "sha256": f.file_sha256,
                    "canonical_hash": f.canonical_hash,
                    "source": f.source,
                    "date": f.date,
                    "batch_id": f.batch_id,
                }
                for f in files
            ],
            "target_partitions_count": len(grouped),
            "target_shards_count": shard_count,
            "replaced_compact_dir": replaced_dir.name if replaced_dir else None,
            "active_layout": LAYOUT_DAILY_JSONL,
            "report_path": report_path.as_posix(),
        }
        atomic_write_bytes(report_path, json.dumps(report_data, indent=2, ensure_ascii=False).encode("utf-8"))
        return report_data


def verify_storage(storage_root: str = "data") -> Dict[str, Any]:
    """Checks that every envelope recorded in the latest migration report is still in raw_compact."""
    root = Path(storage_root)
    store = RawEnvelopeStore(storage_root=storage_root)
    if store.peek_layout() != LAYOUT_DAILY_JSONL or not store.state_file.exists():
        return {"status": "VERIFY_NOT_APPLICABLE", "active_layout": store.peek_layout()}

    report_path = _latest_report(root)
    if report_path is None:
        return {"status": "VERIFY_NOT_APPLICABLE", "message": "no migration report found"}

    report = json.loads(report_path.read_text(encoding="utf-8"))
    expected = collections.Counter(e["canonical_hash"] for e in report.get("migrated_files", []))
    actual = collections.Counter(
        compute_canonical_hash(env) for env in _iter_shard_envelopes(root / COMPACT_DIRNAME)
    )
    missing = expected - actual
    return {
        "status": "VERIFY_MISMATCH" if missing else "VERIFY_SUCCESS",
        "report": report_path.name,
        "expected_envelopes": sum(expected.values()),
        "compact_envelopes": sum(actual.values()),
        "missing_envelopes": sum(missing.values()),
    }


def _resolve_prunable(root: Path, rel_path: str) -> Optional[Path]:
    """Returns the resolved path iff rel_path is a .json strictly inside <root>/raw/<source>/date=*/."""
    try:
        resolved = (root / rel_path).resolve()
        raw_root = (root / "raw").resolve()
        rel = resolved.relative_to(raw_root)
    except (ValueError, OSError):
        return None
    parts = rel.parts
    if len(parts) != 3 or parts[0] not in SUPPORTED_SOURCES or not parts[1].startswith("date="):
        return None
    if resolved.suffix != ".json":
        return None
    return resolved


def prune_legacy_storage(storage_root: str = "data") -> Dict[str, Any]:
    """Archives migrated legacy JSON files into a verified ZIP, then removes only unchanged files."""
    root = Path(storage_root)
    store = RawEnvelopeStore(storage_root=storage_root)

    with store.lock():
        if store.peek_layout() != LAYOUT_DAILY_JSONL or not store.state_file.exists():
            return {
                "status": "ABORTED_LAYOUT_NOT_ACTIVE",
                "message": "Cannot prune legacy files when layout is not daily_jsonl",
            }

        report_path = _latest_report(root)
        if report_path is None:
            return {
                "status": "ABORTED_NO_MIGRATION_REPORT",
                "message": "migration_report_*.json not found in logs/",
            }

        report = json.loads(report_path.read_text(encoding="utf-8"))
        entries = report.get("migrated_files", [])

        to_prune: list[tuple[str, Path, str]] = []  # (rel_path, resolved, sha256)
        skipped: list[dict[str, str]] = []
        for entry in entries:
            rel_path = entry.get("rel_path", "") if isinstance(entry, dict) else ""
            resolved = _resolve_prunable(root, rel_path) if rel_path else None
            if resolved is None:
                skipped.append({"rel_path": str(rel_path), "reason": "invalid_path"})
                continue
            if not resolved.exists():
                continue  # already pruned
            if file_sha256(resolved) != entry.get("sha256"):
                skipped.append({"rel_path": rel_path, "reason": "changed_since_migration"})
                continue
            to_prune.append((rel_path, resolved, entry["sha256"]))

        if not to_prune:
            return {"status": "NOOP", "message": "No unchanged migrated files left to prune", "skipped": skipped}

        backups_dir = root / "backups"
        backups_dir.mkdir(parents=True, exist_ok=True)
        zip_path = backups_dir / BACKUP_ZIP_NAME
        if zip_path.exists():
            zip_path = backups_dir / f"raw-json-before-migration-{_now_stamp()}.zip"
        tmp_zip = zip_path.parent / f"{zip_path.name}.tmp"

        try:
            with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for rel_path, resolved, _sha in to_prune:
                    zf.write(resolved, arcname=rel_path)

            with zipfile.ZipFile(tmp_zip, "r") as zf:
                for rel_path, _resolved, sha in to_prune:
                    if hashlib.sha256(zf.read(rel_path)).hexdigest() != sha:
                        tmp_zip.unlink(missing_ok=True)
                        return {"status": "ABORTED_ZIP_CHECKSUM_MISMATCH", "file": rel_path}
        except Exception:
            tmp_zip.unlink(missing_ok=True)
            raise

        safe_replace(tmp_zip, zip_path)

        deleted = 0
        for _rel, resolved, _sha in to_prune:
            resolved.unlink()
            deleted += 1

        return {
            "status": "PRUNE_SUCCESS",
            "backup_zip": str(zip_path),
            "files_deleted_count": deleted,
            "skipped": skipped,
        }


def _legacy_filename(source: str, env: Dict[str, Any], canonical_hash: str) -> str:
    batch_id = (env.get("metadata") or {}).get("batch_id")
    stem = _SAFE_NAME_RE.sub("_", str(batch_id)) if batch_id else canonical_hash[:12]
    return f"{source}_{stem}.json"


def rollback_storage(storage_root: str = "data") -> Dict[str, Any]:
    """Rolls back storage layout to legacy_files, extracting compact-only batches. Idempotent."""
    root = Path(storage_root)
    store = RawEnvelopeStore(storage_root=storage_root)

    with store.lock():
        extracted = 0
        compact_dir = root / COMPACT_DIRNAME
        for source in SUPPORTED_SOURCES:
            s_dir = compact_dir / source
            if not s_dir.exists():
                continue
            for date_dir in sorted(s_dir.glob("date=*")):
                date_str = date_dir.name.split("=", 1)[1]
                target_dir = root / "raw" / source / f"date={date_str}"

                # Content already present as legacy JSON (by canonical hash) is not re-extracted.
                existing_hashes: set[str] = set()
                if target_dir.exists():
                    for fp in target_dir.glob("*.json"):
                        if is_temp_file(fp.name):
                            continue
                        try:
                            existing_hashes.add(compute_canonical_hash(json.loads(fp.read_text(encoding="utf-8"))))
                        except Exception:
                            continue

                for shard in sorted(date_dir.glob("batches-*.jsonl")):
                    if is_temp_file(shard.name):
                        continue
                    with open(shard, "r", encoding="utf-8") as f:
                        for line in f:
                            if not line.strip():
                                continue
                            env = json.loads(line)
                            h = compute_canonical_hash(env)
                            if h in existing_hashes:
                                continue
                            target_file = target_dir / _legacy_filename(source, env, h)
                            if target_file.exists():
                                target_file = target_dir / f"{target_file.stem}_{h[:8]}.json"
                            atomic_write_bytes(
                                target_file,
                                json.dumps(env, indent=2, ensure_ascii=False).encode("utf-8"),
                            )
                            existing_hashes.add(h)
                            extracted += 1

        store.set_layout(LAYOUT_LEGACY_FILES)

        return {
            "status": "ROLLBACK_SUCCESS",
            "active_layout": LAYOUT_LEGACY_FILES,
            "new_batches_extracted_to_legacy": extracted,
        }


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entrypoint for storage migration tool."""
    parser = argparse.ArgumentParser(description="Compact Raw Storage Migration and Maintenance Tool")
    parser.add_argument("--base-dir", default="data", help="Root storage directory (default: data)")
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument("--dry-run", action="store_true", help="Inspect and validate without modifying disk")
    action_group.add_argument("--apply", action="store_true", help="Migrate legacy files to daily JSONL layout")
    action_group.add_argument("--prune-legacy", action="store_true", help="Backup to ZIP and prune legacy JSON files")
    action_group.add_argument("--rollback", action="store_true", help="Revert layout back to legacy_files")
    action_group.add_argument(
        "--verify", action="store_true", help="Check all migrated envelopes are still present in raw_compact"
    )

    args = parser.parse_args(argv)

    if args.dry_run:
        res = migrate_storage(args.base_dir, action="dry-run")
        ok = res["status"] == "DRY_RUN_SUCCESS"
    elif args.apply:
        res = migrate_storage(args.base_dir, action="apply")
        ok = res["status"] in ("SUCCESS", "ALREADY_ACTIVE")
    elif args.prune_legacy:
        res = prune_legacy_storage(args.base_dir)
        ok = res["status"] in ("PRUNE_SUCCESS", "NOOP")
    elif args.rollback:
        res = rollback_storage(args.base_dir)
        ok = res["status"] == "ROLLBACK_SUCCESS"
    else:
        res = verify_storage(args.base_dir)
        ok = res["status"] in ("VERIFY_SUCCESS", "VERIFY_NOT_APPLICABLE")

    print(json.dumps(res, indent=2, ensure_ascii=False))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
