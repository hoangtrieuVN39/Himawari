import json
import logging
import os
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import requests

from src.ingestion.storage import append_ingestion_log

logger = logging.getLogger(__name__)


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
        log_path: str = "data/logs/hdfs_sync_log.jsonl"
    ) -> Dict[str, Any]:
        """
        Synchronizes files from local data directory (focusing on raw/ and processed/)
        to HDFS, measures elapsed time and throughput (MB/s), and logs audit record.
        """
        if not self.is_available():
            summary = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "status": "SKIPPED_HDFS_OFFLINE",
                "files_uploaded": 0,
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
        target_subdirs = [base / sub for sub in ("raw", "processed") if (base / sub).exists()]
        if not target_subdirs:
            target_subdirs = [base]

        files_to_sync: List[Path] = []
        log_resolved = Path(log_path).resolve() if log_path else None

        for target in target_subdirs:
            if target.is_dir():
                for p in target.rglob("*"):
                    if p.is_file() and not p.name.endswith(".tmp") and not p.name.endswith(".part"):
                        if log_resolved and p.resolve() == log_resolved:
                            continue
                        files_to_sync.append(p)
            elif target.is_file():
                files_to_sync.append(target)

        start_time = time.time()
        files_uploaded = 0
        total_bytes = 0
        failed_count = 0

        for file_path in files_to_sync:
            try:
                rel_path = file_path.relative_to(base).as_posix()
            except ValueError:
                rel_path = file_path.name

            target_hdfs_path = f"{hdfs_root.rstrip('/')}/{rel_path.lstrip('/')}"
            res = self.upload_file(
                str(file_path),
                target_hdfs_path,
                overwrite=True,
                check_availability=False
            )
            if res.get("status") == "SUCCESS":
                files_uploaded += 1
                total_bytes += res.get("bytes_uploaded", 0)
            else:
                failed_count += 1

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
            "total_bytes": total_bytes,
            "elapsed_ms": elapsed_ms,
            "throughput_mb_s": throughput_mb_s,
            "local_dir": str(local_dir),
            "hdfs_root": hdfs_root
        }

        if log_path:
            append_ingestion_log(summary, log_path=log_path)

        return summary

