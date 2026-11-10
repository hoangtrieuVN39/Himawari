"""Shared filesystem helpers: hashing, temp-file detection, and atomic writes."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import time
import uuid


def is_temp_file(name: str) -> bool:
    """True for in-flight artifacts: *.tmp, *.tmp.<suffix>, *.part."""
    return name.endswith((".tmp", ".part")) or ".tmp." in name


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Streams a file and returns its hex SHA-256."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_replace(src: Path, dst: Path, max_retries: int = 10, backoff: float = 0.1) -> None:
    """os.replace with retries for transient Windows sharing violations."""
    for attempt in range(max_retries):
        try:
            os.replace(src, dst)
            return
        except (PermissionError, FileExistsError):
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff)


def atomic_write_bytes(dst: Path, data: bytes) -> None:
    """Writes data to a unique temp file next to dst, fsyncs, then atomically replaces dst."""
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / f"{dst.name}.tmp.{os.getpid()}_{uuid.uuid4().hex[:6]}"
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        safe_replace(tmp, dst)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
