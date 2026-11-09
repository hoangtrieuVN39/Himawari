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
