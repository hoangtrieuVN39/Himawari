import json
import logging
import os
import shutil
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests

from src.ingestion.fs_utils import atomic_write_bytes, file_sha256, is_temp_file
from src.ingestion.raw_store import (
    LAYOUT_DAILY_JSONL,
    CorruptStorageStateError,
    LockTimeoutError,
    RawEnvelopeStore,
)
from src.ingestion.storage import append_ingestion_log

logger = logging.getLogger(__name__)

SNAPSHOT_DIRNAME = ".sync-snapshot"


class WebHDFSClient:
    """
    Dual-mode WebHDFS client supporting availability probing, 2-step REST upload,
    and bulk directory synchronization with empirical throughput logging.
    """

    def __init__(
        self,
        namenode_host: str = "localhost",
        port: int = 9870,
        user: str = "root"
    ):
        self.namenode_host = namenode_host or os.getenv("HDFS_NAMENODE_HOST", "localhost")
        self.port = int(os.getenv("HDFS_PORT", port))
        self.user = user or os.getenv("HDFS_USER", "root")

    @property
    def base_url(self) -> str:
        return f"http://{self.namenode_host}:{self.port}/webhdfs/v1"

    def is_available(self, timeout: float = 1.5) -> bool:
        """
        Probes NameNode WebHDFS endpoint with a short timeout.
        Returns True if reachable and returning HTTP 200, False otherwise.
        """
        probe_url = f"{self.base_url}/?op=GETFILESTATUS&user.name={self.user}"
        try:
            resp = requests.get(probe_url, timeout=timeout)
            return resp.status_code == 200
        except Exception:
            return False

    def upload_file(
        self,
        local_path: str,
        hdfs_path: str,
        overwrite: bool = True,
        check_availability: bool = True
    ) -> Dict[str, Any]:
        """
        Uploads a local file to HDFS using WebHDFS 2-step CREATE flow.
        Step 1: PUT to NameNode with allow_redirects=False.
        Step 2: PUT payload stream to DataNode URL returned in Location header.
        If HDFS is offline, gracefully skips without throwing exceptions.
        """
        local_file = Path(local_path)
        if not local_file.is_file():
            return {
                "status": "FAILED",
                "bytes_uploaded": 0,
                "error": f"Local file not found: {local_path}",
                "local_path": str(local_path),
                "hdfs_path": hdfs_path
            }

        file_size = local_file.stat().st_size

        if check_availability and not self.is_available():
            return {
                "status": "SKIPPED_HDFS_OFFLINE",
                "bytes_uploaded": 0,
                "local_path": str(local_path),
                "hdfs_path": hdfs_path
            }

        normalized_hdfs_path = "/" + hdfs_path.strip().lstrip("/")
        overwrite_val = "true" if overwrite else "false"
        step1_url = (
            f"{self.base_url}{normalized_hdfs_path}"
            f"?op=CREATE&overwrite={overwrite_val}&user.name={self.user}"
        )

        start_time = time.time()
        try:
            step1_resp = requests.put(step1_url, allow_redirects=False, timeout=10.0)
        except requests.exceptions.RequestException as exc:
            return {
                "status": "SKIPPED_HDFS_OFFLINE",
                "bytes_uploaded": 0,
                "error": str(exc),
                "local_path": str(local_path),
                "hdfs_path": hdfs_path
            }

        # Step 2: Handle redirect or direct success
        if step1_resp.status_code == 307:
            location = step1_resp.headers.get("Location")
            if not location:
                return {
                    "status": "FAILED",
                    "bytes_uploaded": 0,
                    "error": "Missing Location header on 307 redirect",
                    "local_path": str(local_path),
                    "hdfs_path": hdfs_path
                }

            # If connecting via localhost/127.0.0.1, rewrite DataNode hostname
            # to match namenode_host if DataNode returned an internal Docker hostname
            if self.namenode_host in ("localhost", "127.0.0.1"):
                parsed = urllib.parse.urlparse(location)
                if parsed.hostname and parsed.hostname not in ("localhost", "127.0.0.1"):
                    location = location.replace(f"//{parsed.hostname}:", f"//{self.namenode_host}:")

            try:
                with local_file.open("rb") as f:
                    step2_resp = requests.put(
                        location,
                        data=f,
                        headers={"Content-Type": "application/octet-stream"},
                        timeout=60.0
                    )
                if step2_resp.status_code in (200, 201):
                    elapsed_ms = round((time.time() - start_time) * 1000, 2)
                    return {
                        "status": "SUCCESS",
                        "bytes_uploaded": file_size,
                        "local_path": str(local_path),
                        "hdfs_path": hdfs_path,
                        "elapsed_ms": elapsed_ms
                    }
                else:
                    return {
                        "status": "FAILED",
                        "bytes_uploaded": 0,
                        "error": f"DataNode upload error {step2_resp.status_code}: {step2_resp.text}",
                        "local_path": str(local_path),
                        "hdfs_path": hdfs_path
                    }
            except requests.exceptions.RequestException as exc:
                return {
                    "status": "FAILED",
                    "bytes_uploaded": 0,
                    "error": str(exc),
                    "local_path": str(local_path),
                    "hdfs_path": hdfs_path
                }

        elif step1_resp.status_code in (200, 201):
            elapsed_ms = round((time.time() - start_time) * 1000, 2)
            return {
                "status": "SUCCESS",
                "bytes_uploaded": file_size,
                "local_path": str(local_path),
                "hdfs_path": hdfs_path,
                "elapsed_ms": elapsed_ms
            }
        else:
            return {
                "status": "FAILED",
                "bytes_uploaded": 0,
                "error": f"NameNode CREATE failed with status {step1_resp.status_code}: {step1_resp.text}",
                "local_path": str(local_path),
                "hdfs_path": hdfs_path
            }

    def sync_directory(
        self,
        local_dir: str = "data",
        hdfs_root: str = "/data",
        log_path: str = "data/logs/hdfs_sync_log.jsonl",
        lock_timeout_sec: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Synchronizes files from local data directory (focusing on active raw layout and processed/)
        to HDFS, tracks checksum manifest, measures elapsed time and throughput, and logs audit record.

        Only the active raw layout is synced (never a union of raw/ and raw_compact/). Mutable compact
        shards and the layout marker are copied into a temporary snapshot under the storage lock, and
        uploaded from that snapshot.
        """
        if not self.is_available():
            summary = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "status": "SKIPPED_HDFS_OFFLINE",
                "files_uploaded": 0,
                "files_skipped_unchanged": 0,
                "total_bytes": 0,
                "elapsed_ms": 0.0,
                "throughput_mb_s": 0.0,
                "local_dir": str(local_dir),
                "hdfs_root": hdfs_root
            }
            if log_path:
                append_ingestion_log(summary, log_path=log_path)
            return summary

        base = Path(local_dir)
        store = RawEnvelopeStore(storage_root=str(base), lock_timeout_sec=lock_timeout_sec)
        snapshot_dir = base / SNAPSHOT_DIRNAME
        errors: List[str] = []

        def _failed_summary(status: str, error: str) -> Dict[str, Any]:
            summary = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "files_uploaded": 0,
                "files_skipped_unchanged": 0,
                "total_bytes": 0,
                "elapsed_ms": 0.0,
                "throughput_mb_s": 0.0,
                "local_dir": str(local_dir),
                "hdfs_root": hdfs_root,
                "errors": [error],
            }
            if log_path:
                append_ingestion_log(summary, log_path=log_path)
            return summary

        try:
            # 1. Snapshot the active layout's candidate files under the storage lock.
            try:
                with store.lock():
                    items, marker_item = self._collect_sync_items(base, store, snapshot_dir)
            except LockTimeoutError as exc:
                return _failed_summary("FAILED_LOCK_TIMEOUT", str(exc))
            except CorruptStorageStateError as exc:
                return _failed_summary("FAILED_CORRUPT_STORAGE_STATE", str(exc))

            # 2. Read local manifest
            manifest_path = base / "logs" / "hdfs_sync_manifest.json"
            manifest: dict[str, str] = {}
            if manifest_path.exists():
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                except Exception as exc:
                    errors.append(f"manifest unreadable, re-uploading everything: {exc}")
                    manifest = {}

            # 3. Upload files (outside process lock)
            start_time = time.time()
            files_uploaded = 0
            files_skipped_unchanged = 0
            total_bytes = 0
            failed_count = 0
            updated_manifest = dict(manifest)

            def _upload(upload_path: Path, rel_path: str) -> str:
                """Returns 'uploaded' | 'skipped' | 'failed'. Records errors."""
                nonlocal files_uploaded, files_skipped_unchanged, total_bytes
                target = f"{hdfs_root.rstrip('/')}/{rel_path.lstrip('/')}"
                try:
                    sha = file_sha256(upload_path)
                    if manifest.get(target) == sha:
                        files_skipped_unchanged += 1
                        return "skipped"
                    res = self.upload_file(str(upload_path), target, overwrite=True, check_availability=False)
                except Exception as exc:
                    errors.append(f"{rel_path}: {exc}")
                    return "failed"
                if res.get("status") == "SUCCESS":
                    files_uploaded += 1
                    total_bytes += res.get("bytes_uploaded", 0)
                    updated_manifest[target] = sha
                    return "uploaded"
                errors.append(f"{rel_path}: {res.get('error') or res.get('status')}")
                return "failed"

            for upload_path, rel_path in items:
                if _upload(upload_path, rel_path) == "failed":
                    failed_count += 1

            # 4. Marker cutover: only upload storage_state.json if all other files succeeded
            if marker_item is not None and failed_count == 0:
                if _upload(*marker_item) == "failed":
                    failed_count += 1
        finally:
            shutil.rmtree(snapshot_dir, ignore_errors=True)

        # 5. Atomically update manifest file
        if updated_manifest != manifest:
            try:
                atomic_write_bytes(manifest_path, json.dumps(updated_manifest, indent=2).encode("utf-8"))
            except Exception as exc:
                errors.append(f"manifest write failed: {exc}")
                logger.warning("Could not write HDFS sync manifest: %s", exc)

        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        throughput_mb_s = (
            round((total_bytes / (1024 * 1024)) / (elapsed_ms / 1000.0), 4)
            if elapsed_ms > 0
            else 0.0
        )

        status = "SUCCESS" if failed_count == 0 else ("PARTIAL" if files_uploaded > 0 else "FAILED")
        summary = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "files_uploaded": files_uploaded,
            "files_skipped_unchanged": files_skipped_unchanged,
            "total_bytes": total_bytes,
            "elapsed_ms": elapsed_ms,
            "throughput_mb_s": throughput_mb_s,
            "local_dir": str(local_dir),
            "hdfs_root": hdfs_root,
            "errors": errors,
        }

        if log_path:
            append_ingestion_log(summary, log_path=log_path)

        return summary

    @staticmethod
    def _collect_sync_items(
        base: Path,
        store: RawEnvelopeStore,
        snapshot_dir: Path,
    ) -> Tuple[List[Tuple[Path, str]], Optional[Tuple[Path, str]]]:
        """Lists (upload_path, rel_path) for the ACTIVE layout only. Caller must hold the storage lock.

        Mutable files (compact shards, layout marker) are copied into snapshot_dir; immutable or
        atomically-replaced files (HSD binaries, processed/, legacy JSON) are referenced in place.
        """
        layout = store.peek_layout()
        items: List[Tuple[Path, str]] = []

        def add_tree(tree: Path, copy: bool) -> None:
            if not tree.exists():
                return
            for p in sorted(tree.rglob("*")):
                if not p.is_file() or is_temp_file(p.name):
                    continue
                rel = p.relative_to(base).as_posix()
                if copy:
                    dst = snapshot_dir / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(p, dst)
                    items.append((dst, rel))
                else:
                    items.append((p, rel))

        if layout == LAYOUT_DAILY_JSONL:
            add_tree(base / "raw_compact", copy=True)
            add_tree(base / "raw" / "himawari_hsd", copy=False)
            add_tree(base / "raw" / "synthetic_quarantine", copy=False)
        else:
            add_tree(base / "raw", copy=False)
        add_tree(base / "processed", copy=False)

        marker_item: Optional[Tuple[Path, str]] = None
        if store.state_file.exists():
            rel = store.state_file.relative_to(base).as_posix()
            dst = snapshot_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(store.state_file, dst)
            marker_item = (dst, rel)

        return items, marker_item

