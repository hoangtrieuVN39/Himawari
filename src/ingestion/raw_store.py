"""RawEnvelopeStore: Centralized storage abstraction for Daily JSONL and Legacy files.

Features:
- Partitioning by date: date=YYYY-MM-DD
- Sharding by size: batches-0001.jsonl, rollover at shard_max_bytes (default 64 MiB)
- Atomic snapshot rewrite via temp file, flush, fsync, and os.replace
- Cross-process exclusive file locking via portalocker (timeout 10.0s)
- Idempotent writes with batch_id verification and BatchConflictError detection
- Single active layout enforcement (storage_state.json): daily_jsonl vs legacy_files
- Deterministic state signature for cache invalidation
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import portalocker

from src.ingestion.fs_utils import atomic_write_bytes, is_temp_file, safe_replace

logger = logging.getLogger(__name__)

SUPPORTED_SOURCES = ("metar", "open_meteo", "himawari_metrics")

LAYOUT_DAILY_JSONL = "daily_jsonl"
LAYOUT_LEGACY_FILES = "legacy_files"
LAYOUTS = (LAYOUT_DAILY_JSONL, LAYOUT_LEGACY_FILES)
LOCK_FILENAME = "raw_store.lock"


class RawStoreError(Exception):
    """Base exception for all raw store errors."""
    pass


class LockTimeoutError(RawStoreError):
    """Raised when exclusive process lock cannot be acquired within timeout window."""
    pass


class BatchConflictError(RawStoreError):
    """Raised when an envelope has the same batch_id but conflicting payload content."""
    pass


class CorruptStorageStateError(RawStoreError):
    """Raised when storage_state.json is malformed or invalid."""
    pass


class CorruptShardError(RawStoreError):
    """Raised when a JSONL line cannot be parsed in strict mode."""
    pass


@dataclass(frozen=True)
class WriteReceipt:
    path: str
    batch_id: str
    bytes_added: int
    inserted: bool
    shard_index: int = 1
    total_shard_bytes: int = 0


@dataclass(frozen=True)
class ReadSpec:
    paths: Tuple[str, ...]
    multiline: bool
    layout: str
    source: str


class RawEnvelopeStore:
    def __init__(
        self,
        storage_root: str = "data",
        lock_timeout_sec: float = 10.0,
        shard_max_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        self.storage_root = Path(storage_root)
        self.lock_timeout_sec = float(lock_timeout_sec)
        self.shard_max_bytes = int(shard_max_bytes)
        self.locks_dir = self.storage_root / ".storage-locks"
        self.state_file = self.storage_root / "storage_state.json"

    @contextmanager
    def lock(self) -> Iterator[None]:
        """Exclusive cross-process lock. Raises LockTimeoutError if not acquired in lock_timeout_sec.

        Not re-entrant: do not nest within the same process.
        """
        self.locks_dir.mkdir(parents=True, exist_ok=True)
        lock = portalocker.Lock(str(self.locks_dir / LOCK_FILENAME), timeout=self.lock_timeout_sec)
        try:
            lock.acquire()
        except portalocker.exceptions.LockException as exc:  # AlreadyLocked is a subclass
            raise LockTimeoutError(f"Could not acquire storage lock within {self.lock_timeout_sec}s") from exc
        try:
            yield
        finally:
            lock.release()

    def _acquire_lock(self):
        """Backward-compatible alias for lock()."""
        return self.lock()

    def peek_layout(self) -> str:
        """Returns the active layout without any side effect (never creates the marker)."""
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text(encoding="utf-8"))
            except Exception as exc:
                raise CorruptStorageStateError(f"Invalid JSON in {self.state_file}") from exc

            layout = data.get("raw_layout")
            if layout not in LAYOUTS:
                raise CorruptStorageStateError(f"Invalid raw_layout '{layout}' in {self.state_file}")
            return layout

        # No marker exists: auto-detect
        legacy_dir = self.storage_root / "raw"
        if legacy_dir.exists() and any(legacy_dir.rglob("*.json")):
            return LAYOUT_LEGACY_FILES
        return LAYOUT_DAILY_JSONL

    def active_layout(self) -> str:
        """Returns the active layout; on a fresh root, initializes the marker under lock."""
        layout = self.peek_layout()
        if layout == LAYOUT_DAILY_JSONL and not self.state_file.exists():
            with self.lock():
                self._ensure_marker_unlocked()
        return layout

    def _ensure_marker_unlocked(self) -> str:
        """Creates the daily_jsonl marker on a fresh root. Caller must hold lock(). Returns active layout."""
        layout = self.peek_layout()
        if layout == LAYOUT_DAILY_JSONL and not self.state_file.exists():
            self.set_layout(LAYOUT_DAILY_JSONL)
        return layout

    def set_layout(self, layout: str) -> None:
        """Atomically writes the storage_state.json marker. Caller should hold lock()."""
        if layout not in LAYOUTS:
            raise ValueError(f"Invalid layout: {layout}")
        marker_data = {
            "schema_version": 1,
            "raw_layout": layout,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "active_sources": list(SUPPORTED_SOURCES),
            "shard_max_bytes": self.shard_max_bytes,
        }
        atomic_write_bytes(
            self.state_file,
            json.dumps(marker_data, indent=2, ensure_ascii=False).encode("utf-8"),
        )

    def _initialize_marker(self, layout: str) -> None:
        """Backward-compatible alias for set_layout()."""
        self.set_layout(layout)

    def write(
        self,
        source: str,
        date_str: str,
        envelope: Dict[str, Any],
    ) -> WriteReceipt:
        """Atomically writes an envelope into the active layout."""
        if source not in SUPPORTED_SOURCES:
            raise ValueError(f"Unsupported source: '{source}'. Supported: {SUPPORTED_SOURCES}")

        metadata = envelope.get("metadata") or {}
        batch_id = metadata.get("batch_id")
        if not batch_id:
            raise ValueError("Envelope must contain metadata.batch_id")

        with self.lock():
            layout = self._ensure_marker_unlocked()

            if layout == LAYOUT_LEGACY_FILES:
                return self._write_legacy(source, date_str, envelope, batch_id)
            else:
                return self._write_compact(source, date_str, envelope, batch_id)

    def _write_legacy(
        self,
        source: str,
        date_str: str,
        envelope: Dict[str, Any],
        batch_id: str,
    ) -> WriteReceipt:
        target_dir = self.storage_root / "raw" / source / f"date={date_str}"
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{source}_{batch_id}.json"
        target_file = target_dir / filename
        if target_file.exists():
            # Check idempotency
            try:
                existing = json.loads(target_file.read_text(encoding="utf-8"))
                if self._payloads_equal(existing.get("payload"), envelope.get("payload")):
                    return WriteReceipt(
                        path=str(target_file),
                        batch_id=batch_id,
                        bytes_added=0,
                        inserted=False,
                        shard_index=1,
                        total_shard_bytes=target_file.stat().st_size,
                    )
                else:
                    raise BatchConflictError(f"Legacy batch '{batch_id}' already exists with different payload")
            except json.JSONDecodeError:
                pass

        encoded = json.dumps(envelope, ensure_ascii=False, indent=2).encode("utf-8")
        atomic_write_bytes(target_file, encoded)
        return WriteReceipt(
            path=str(target_file),
            batch_id=batch_id,
            bytes_added=len(encoded),
            inserted=True,
            shard_index=1,
            total_shard_bytes=len(encoded),
        )

    def _write_compact(
        self,
        source: str,
        date_str: str,
        envelope: Dict[str, Any],
        batch_id: str,
    ) -> WriteReceipt:
        target_dir = self.storage_root / "raw_compact" / source / f"date={date_str}"
        target_dir.mkdir(parents=True, exist_ok=True)

        line_str = json.dumps(envelope, ensure_ascii=False) + "\n"
        line_bytes = line_str.encode("utf-8")
        line_len = len(line_bytes)

        # 1. Scan existing shards in partition to check idempotency and pick target shard
        existing_shards = sorted(target_dir.glob("batches-*.jsonl"))
        
        for shard in existing_shards:
            for existing_env in self._iter_jsonl_file(shard, strict=False):
                meta = existing_env.get("metadata") or {}
                if meta.get("batch_id") == batch_id:
                    if self._payloads_equal(existing_env.get("payload"), envelope.get("payload")):
                        # No-op retry
                        return WriteReceipt(
                            path=str(shard),
                            batch_id=batch_id,
                            bytes_added=0,
                            inserted=False,
                            shard_index=self._extract_shard_index(shard.name),
                            total_shard_bytes=shard.stat().st_size,
                        )
                    else:
                        raise BatchConflictError(
                            f"Batch '{batch_id}' already exists in {shard.name} with different payload"
                        )

        # 2. Determine shard rollover
        if not existing_shards:
            shard_idx = 1
            target_shard = target_dir / f"batches-{shard_idx:04d}.jsonl"
            shard_existing_bytes = b""
        else:
            latest_shard = existing_shards[-1]
            curr_size = latest_shard.stat().st_size
            shard_idx = self._extract_shard_index(latest_shard.name)

            if curr_size + line_len > self.shard_max_bytes and curr_size > 0:
                shard_idx += 1
                target_shard = target_dir / f"batches-{shard_idx:04d}.jsonl"
                shard_existing_bytes = b""
            else:
                target_shard = latest_shard
                with open(target_shard, "rb") as f:
                    shard_existing_bytes = f.read()

        # 3. Snapshot rewrite via temp file
        atomic_write_bytes(target_shard, shard_existing_bytes + line_bytes)
        total_size = len(shard_existing_bytes) + line_len

        return WriteReceipt(
            path=str(target_shard),
            batch_id=batch_id,
            bytes_added=line_len,
            inserted=True,
            shard_index=shard_idx,
            total_shard_bytes=total_size,
        )

    def iter_envelopes(
        self,
        source: str,
        *,
        date_str: Optional[str] = None,
        strict: bool = True,
    ) -> Iterator[Dict[str, Any]]:
        """Iterates over envelopes in the active layout."""
        layout = self.peek_layout()

        if layout == LAYOUT_DAILY_JSONL:
            base_dir = self.storage_root / "raw_compact" / source
            if not base_dir.exists():
                return

            if date_str:
                pattern = f"date={date_str}/batches-*.jsonl"
            else:
                pattern = "date=*/batches-*.jsonl"

            for shard_path in sorted(base_dir.glob(pattern)):
                yield from self._iter_jsonl_file(shard_path, strict=strict)

        else:
            base_dir = self.storage_root / "raw" / source
            if not base_dir.exists():
                return

            if date_str:
                pattern = f"date={date_str}/*.json"
            else:
                pattern = "date=*/*.json"

            for f_path in sorted(base_dir.glob(pattern)):
                if is_temp_file(f_path.name):
                    continue
                try:
                    yield json.loads(f_path.read_text(encoding="utf-8"))
                except Exception as exc:
                    if strict:
                        raise CorruptShardError(f"Corrupt JSON file: {f_path}") from exc
                    logger.warning("Skipping corrupt JSON file: %s (%s)", f_path, exc)

    def _iter_jsonl_file(self, shard_path: Path, strict: bool = True) -> Iterator[Dict[str, Any]]:
        with open(shard_path, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                clean_line = line.strip()
                if not clean_line:
                    continue
                try:
                    yield json.loads(clean_line)
                except Exception as exc:
                    if strict:
                        raise CorruptShardError(
                            f"Corrupt line {line_no} in shard {shard_path}: {clean_line[:80]}"
                        ) from exc
                    logger.warning(
                        "Skipping corrupt line %d in %s: %s",
                        line_no,
                        shard_path,
                        clean_line[:80],
                    )

    def read_specs(
        self,
        source: str,
        date_str: Optional[str] = None,
    ) -> List[ReadSpec]:
        """Returns ReadSpec describing paths and multiline configuration for readers."""
        layout = self.peek_layout()
        if layout == LAYOUT_DAILY_JSONL:
            base_dir = self.storage_root / "raw_compact" / source
            pattern = f"date={date_str}/batches-*.jsonl" if date_str else "date=*/batches-*.jsonl"
            paths = tuple(
                sorted(p.as_posix() for p in base_dir.glob(pattern) if not is_temp_file(p.name))
            ) if base_dir.exists() else ()
            return [ReadSpec(paths=paths, multiline=False, layout=layout, source=source)]
        else:
            base_dir = self.storage_root / "raw" / source
            pattern = f"date={date_str}/*.json" if date_str else "date=*/*.json"
            paths = tuple(
                sorted(p.as_posix() for p in base_dir.glob(pattern) if not is_temp_file(p.name))
            ) if base_dir.exists() else ()
            return [ReadSpec(paths=paths, multiline=True, layout=layout, source=source)]

    def signature(self) -> Tuple[Tuple[str, int, int], ...]:
        """Computes a deterministic signature of all active files and state marker."""
        sig_entries: list[tuple[str, int, int]] = []
        if self.state_file.exists():
            try:
                st = self.state_file.stat()
                sig_entries.append(("storage_state.json", st.st_size, st.st_mtime_ns))
            except Exception:
                pass

        layout = self.peek_layout()
        if layout == LAYOUT_DAILY_JSONL:
            base_dir = self.storage_root / "raw_compact"
            pattern = "*/*/*.jsonl"
        else:
            base_dir = self.storage_root / "raw"
            pattern = "*/*/*.json"

        if base_dir.exists():
            for f in base_dir.glob(pattern):
                if f.is_file() and not is_temp_file(f.name):
                    try:
                        rel = f.relative_to(self.storage_root).as_posix()
                        st = f.stat()
                        sig_entries.append((rel, st.st_size, st.st_mtime_ns))
                    except Exception:
                        continue

        sig_entries.sort(key=lambda x: x[0])
        return tuple(sig_entries)

    @staticmethod
    def _payloads_equal(p1: Any, p2: Any) -> bool:
        s1 = json.dumps(p1, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        s2 = json.dumps(p2, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return s1 == s2

    @staticmethod
    def _extract_shard_index(filename: str) -> int:
        try:
            # batches-0001.jsonl
            parts = filename.replace(".jsonl", "").split("-")
            return int(parts[-1])
        except Exception:
            return 1

    @staticmethod
    def _safe_replace(src: Path, dst: Path, max_retries: int = 10, backoff: float = 0.1) -> None:
        """Backward-compatible alias for fs_utils.safe_replace."""
        safe_replace(src, dst, max_retries=max_retries, backoff=backoff)

