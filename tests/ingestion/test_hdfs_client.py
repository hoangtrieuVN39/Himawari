import json
from unittest.mock import Mock, call
import pytest
import requests
from src.ingestion.hdfs_client import WebHDFSClient


def test_hdfs_upload_when_offline_gracefully_skips(tmp_path, mocker):
    sample = tmp_path / "test.json"
    sample.write_text('{"a": 1}', encoding="utf-8")
    mocker.patch("requests.get", side_effect=ConnectionError("Connection refused"))

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    assert client.is_available() is False
    res = client.upload_file(str(sample), "/data/raw/test.json")
    assert res["status"] == "SKIPPED_HDFS_OFFLINE"
    assert res["bytes_uploaded"] == 0


def test_hdfs_sync_directory_records_benchmark_log(tmp_path, mocker):
    raw_dir = tmp_path / "data" / "raw" / "metar" / "date=2026-11-09"
    raw_dir.mkdir(parents=True)
    f1 = raw_dir / "sample.json"
    f1.write_text('{"hello": "hdfs"}', encoding="utf-8")
    log_file = tmp_path / "data" / "logs" / "hdfs_sync_log.jsonl"

    mock_get = mocker.patch("requests.get")
    mock_get.return_value = Mock(status_code=200)
    mock_put = mocker.patch("requests.put")
    mock_put.return_value = Mock(status_code=201, headers={})

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    summary = client.sync_directory(
        local_dir=str(tmp_path / "data"),
        hdfs_root="/data",
        log_path=str(log_file)
    )

    assert summary["status"] == "SUCCESS"
    assert summary["files_uploaded"] >= 1
    assert summary["total_bytes"] >= 17
    assert log_file.exists()
    logged = json.loads(log_file.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert logged["files_uploaded"] >= 1
    assert "elapsed_ms" in logged


def test_hdfs_upload_two_step_redirect_success(tmp_path, mocker):
    sample = tmp_path / "data.csv"
    content = b"col1,col2\nval1,val2\n"
    sample.write_bytes(content)

    mocker.patch("requests.get", return_value=Mock(status_code=200))

    step1_resp = Mock(status_code=307, headers={"Location": "http://datanode:9864/webhdfs/v1/data/data.csv?op=CREATE"})
    step2_resp = Mock(status_code=201)

    mock_put = mocker.patch("requests.put", side_effect=[step1_resp, step2_resp])

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    res = client.upload_file(str(sample), "/data/data.csv")

    assert res["status"] == "SUCCESS"
    assert res["bytes_uploaded"] == len(content)
    assert mock_put.call_count == 2
    # Check that datanode hostname was rewritten to localhost because namenode_host is localhost
    called_url = mock_put.call_args_list[1][0][0]
    assert "localhost:9864" in called_url


def test_hdfs_upload_missing_local_file():
    client = WebHDFSClient(namenode_host="localhost", port=9870)
    res = client.upload_file("non_existent_file.json", "/data/none.json")
    assert res["status"] == "FAILED"
    assert "not found" in res["error"].lower()


def test_hdfs_is_available_status_codes(mocker):
    client = WebHDFSClient(namenode_host="localhost", port=9870)

    mocker.patch("requests.get", return_value=Mock(status_code=200))
    assert client.is_available() is True

    mocker.patch("requests.get", return_value=Mock(status_code=500))
    assert client.is_available() is False

    mocker.patch("requests.get", side_effect=requests.exceptions.Timeout("timed out"))
    assert client.is_available() is False


def test_hdfs_sync_directory_offline_graceful(tmp_path, mocker):
    mocker.patch("requests.get", side_effect=requests.exceptions.ConnectionError("offline"))
    log_file = tmp_path / "data" / "logs" / "hdfs_sync_log.jsonl"

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    summary = client.sync_directory(
        local_dir=str(tmp_path / "data"),
        hdfs_root="/data",
        log_path=str(log_file)
    )

    assert summary["status"] == "SKIPPED_HDFS_OFFLINE"
    assert summary["files_uploaded"] == 0


def test_sync_only_active_storage_layout(tmp_path, mocker):
    """TC-HDFS-layout: syncs only active layout (raw_compact/ not raw/ when compact)."""
    data_dir = tmp_path / "data"
    # Create compact shard
    c_dir = data_dir / "raw_compact" / "metar" / "date=2026-11-09"
    c_dir.mkdir(parents=True)
    (c_dir / "batches-0001.jsonl").write_text('{"compact": true}\n', encoding="utf-8")

    # Create old raw file and backup zip (must NOT be uploaded)
    old_dir = data_dir / "raw" / "metar" / "date=2026-11-09"
    old_dir.mkdir(parents=True)
    (old_dir / "old.json").write_text('{"old": true}', encoding="utf-8")
    b_dir = data_dir / "backups"
    b_dir.mkdir(parents=True)
    (b_dir / "backup.zip").write_bytes(b"ZIP")

    # Set storage_state.json to daily_jsonl
    (data_dir / "storage_state.json").write_text(json.dumps({"raw_layout": "daily_jsonl"}), encoding="utf-8")

    mocker.patch("requests.get", return_value=Mock(status_code=200))
    uploaded_targets = []

    def mock_upload(local_path, hdfs_path, **kwargs):
        uploaded_targets.append(hdfs_path)
        return {"status": "SUCCESS", "bytes_uploaded": 100}

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    client.upload_file = mock_upload

    summary = client.sync_directory(local_dir=str(data_dir), hdfs_root="/hdfs_data", log_path="")
    assert summary["status"] == "SUCCESS"
    assert any("raw_compact/metar/date=2026-11-09/batches-0001.jsonl" in t for t in uploaded_targets)
    assert any("storage_state.json" in t for t in uploaded_targets)
    assert not any("old.json" in t for t in uploaded_targets)
    assert not any("backup.zip" in t for t in uploaded_targets)


def test_unchanged_second_sync_skips_upload_via_manifest(tmp_path, mocker):
    """TC-HDFS-01: second sync with unchanged data skips upload via manifest."""
    data_dir = tmp_path / "data"
    c_dir = data_dir / "raw_compact" / "metar" / "date=2026-11-09"
    c_dir.mkdir(parents=True)
    shard = c_dir / "batches-0001.jsonl"
    shard.write_text('{"batch": 1}\n', encoding="utf-8")
    (data_dir / "storage_state.json").write_text(json.dumps({"raw_layout": "daily_jsonl"}), encoding="utf-8")

    mocker.patch("requests.get", return_value=Mock(status_code=200))
    upload_count = 0

    def mock_upload(local_path, hdfs_path, **kwargs):
        nonlocal upload_count
        upload_count += 1
        return {"status": "SUCCESS", "bytes_uploaded": 50}

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    client.upload_file = mock_upload

    # 1. First sync uploads files
    s1 = client.sync_directory(local_dir=str(data_dir), hdfs_root="/data", log_path="")
    assert s1["status"] == "SUCCESS"
    assert s1["files_uploaded"] == 2
    assert s1["files_skipped_unchanged"] == 0
    assert upload_count == 2

    # 2. Second sync skips all files
    s2 = client.sync_directory(local_dir=str(data_dir), hdfs_root="/data", log_path="")
    assert s2["status"] == "SUCCESS"
    assert s2["files_uploaded"] == 0
    assert s2["files_skipped_unchanged"] == 2
    assert upload_count == 2  # No new upload calls!

    # 3. Modify shard: only modified shard is uploaded
    shard.write_text('{"batch": 1}\n{"batch": 2}\n', encoding="utf-8")
    s3 = client.sync_directory(local_dir=str(data_dir), hdfs_root="/data", log_path="")
    assert s3["status"] == "SUCCESS"
    assert s3["files_uploaded"] == 1
    assert s3["files_skipped_unchanged"] == 1
    assert upload_count == 3


def test_hdfs_remote_marker_cutover_safety(tmp_path, mocker):
    """TC-HDFS-02: marker is not uploaded if a file upload fails."""
    data_dir = tmp_path / "data"
    c_dir = data_dir / "raw_compact" / "metar" / "date=2026-11-09"
    c_dir.mkdir(parents=True)
    (c_dir / "batches-0001.jsonl").write_text('{"batch": 1}\n', encoding="utf-8")
    (data_dir / "storage_state.json").write_text(json.dumps({"raw_layout": "daily_jsonl"}), encoding="utf-8")

    mocker.patch("requests.get", return_value=Mock(status_code=200))
    uploaded_targets = []

    def mock_upload(local_path, hdfs_path, **kwargs):
        if "batches-0001.jsonl" in hdfs_path:
            return {"status": "FAILED", "error": "Connection reset by peer"}
        uploaded_targets.append(hdfs_path)
        return {"status": "SUCCESS", "bytes_uploaded": 50}

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    client.upload_file = mock_upload

    summary = client.sync_directory(local_dir=str(data_dir), hdfs_root="/data", log_path="")
    assert summary["status"] == "FAILED"
    # Marker must NOT be uploaded!
    assert not any("storage_state.json" in t for t in uploaded_targets)


def _make_compact_root(tmp_path):
    data_dir = tmp_path / "data"
    c_dir = data_dir / "raw_compact" / "metar" / "date=2026-11-09"
    c_dir.mkdir(parents=True)
    (c_dir / "batches-0001.jsonl").write_text('{"batch": 1}\n', encoding="utf-8")
    (data_dir / "storage_state.json").write_text(json.dumps({"raw_layout": "daily_jsonl"}), encoding="utf-8")
    return data_dir, c_dir


def test_sync_ignores_unique_temp_files(tmp_path, mocker):
    """Leftover *.tmp.<pid>_<uuid> and *.part files from crashed writers must never be uploaded."""
    data_dir, c_dir = _make_compact_root(tmp_path)
    (c_dir / "batches-0001.jsonl.tmp.1234_ab12cd").write_text("partial", encoding="utf-8")
    (c_dir / "batches-0002.jsonl.part").write_text("partial", encoding="utf-8")

    mocker.patch("requests.get", return_value=Mock(status_code=200))
    uploaded = []
    client = WebHDFSClient(namenode_host="localhost", port=9870)
    client.upload_file = lambda lp, hp, **kw: (uploaded.append(hp), {"status": "SUCCESS", "bytes_uploaded": 1})[1]

    summary = client.sync_directory(local_dir=str(data_dir), hdfs_root="/data", log_path="")
    assert summary["status"] == "SUCCESS"
    assert not any(".tmp" in t or ".part" in t for t in uploaded)


def test_sync_lock_timeout_does_not_fall_back_to_union(tmp_path, mocker):
    """If the storage lock cannot be taken, nothing is uploaded (no raw+raw_compact union)."""
    from src.ingestion.raw_store import RawEnvelopeStore

    data_dir, _ = _make_compact_root(tmp_path)
    old_dir = data_dir / "raw" / "metar" / "date=2026-11-09"
    old_dir.mkdir(parents=True)
    (old_dir / "old.json").write_text('{"old": true}', encoding="utf-8")

    mocker.patch("requests.get", return_value=Mock(status_code=200))
    uploaded = []
    client = WebHDFSClient(namenode_host="localhost", port=9870)
    client.upload_file = lambda lp, hp, **kw: (uploaded.append(hp), {"status": "SUCCESS", "bytes_uploaded": 1})[1]

    with RawEnvelopeStore(str(data_dir)).lock():
        summary = client.sync_directory(
            local_dir=str(data_dir), hdfs_root="/data", log_path="", lock_timeout_sec=0.2
        )

    assert summary["status"] == "FAILED_LOCK_TIMEOUT"
    assert uploaded == []


def test_sync_uploads_from_snapshot_and_cleans_it_up(tmp_path, mocker):
    data_dir, c_dir = _make_compact_root(tmp_path)
    live_shard = (c_dir / "batches-0001.jsonl").resolve()

    mocker.patch("requests.get", return_value=Mock(status_code=200))
    seen = []

    def mock_upload(local_path, hdfs_path, **kwargs):
        from pathlib import Path
        seen.append((Path(local_path).resolve(), Path(local_path).read_text(encoding="utf-8") if "jsonl" in hdfs_path else None))
        return {"status": "SUCCESS", "bytes_uploaded": 1}

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    client.upload_file = mock_upload
    client.sync_directory(local_dir=str(data_dir), hdfs_root="/data", log_path="")

    shard_uploads = [p for p, text in seen if text is not None]
    assert shard_uploads and all(p != live_shard for p in shard_uploads)
    assert not (data_dir / ".sync-snapshot").exists()


def test_sync_reports_marker_upload_exception(tmp_path, mocker):
    data_dir, _ = _make_compact_root(tmp_path)
    mocker.patch("requests.get", return_value=Mock(status_code=200))

    def mock_upload(local_path, hdfs_path, **kwargs):
        if hdfs_path.endswith("storage_state.json"):
            raise RuntimeError("boom")
        return {"status": "SUCCESS", "bytes_uploaded": 1}

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    client.upload_file = mock_upload
    summary = client.sync_directory(local_dir=str(data_dir), hdfs_root="/data", log_path="")

    assert summary["status"] != "SUCCESS"
    assert any("storage_state.json" in e for e in summary["errors"])


def test_sync_reports_manifest_write_failure(tmp_path, mocker):
    data_dir, _ = _make_compact_root(tmp_path)
    mocker.patch("requests.get", return_value=Mock(status_code=200))
    mocker.patch("src.ingestion.hdfs_client.atomic_write_bytes", side_effect=OSError("disk full"))

    client = WebHDFSClient(namenode_host="localhost", port=9870)
    client.upload_file = lambda lp, hp, **kw: {"status": "SUCCESS", "bytes_uploaded": 1}
    summary = client.sync_directory(local_dir=str(data_dir), hdfs_root="/data", log_path="")

    assert any("manifest" in e for e in summary["errors"])
