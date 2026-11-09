import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from src.ingestion.himawari_client import (
    build_s3_segment_url,
    find_latest_available_slot,
    download_himawari_slot,
    cleanup_old_raw_segments,
)


def test_build_s3_segment_url_formats_exact_noaa_key():
    dt = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)
    url, filename = build_s3_segment_url(dt, segment="S0410", band="B13")
    assert filename == "HS_H09_20261109_0800_B13_FLDK_R20_S0410.DAT.bz2"
    assert url == (
        "https://noaa-himawari9.s3.amazonaws.com/"
        "AHI-L1b-FLDK/2026/11/09/0800/HS_H09_20261109_0800_B13_FLDK_R20_S0410.DAT.bz2"
    )


def test_download_himawari_slot_partial_and_success(tmp_path, mocker):
    dt = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)
    mock_fetch = mocker.patch("src.ingestion.himawari_client.fetch_with_retry")

    ok_resp = Mock(status_code=200, content=b"BZh91AY&SY_FAKE_HSD_BYTES")
    mock_fetch.side_effect = [ok_resp, Exception("404 Not Found")]

    res = download_himawari_slot(dt, segments=["S0410", "S0510"], base_dir=str(tmp_path))
    assert res["status"] == "PARTIAL"
    assert res["expected_segments"] == 2
    assert res["downloaded_segments"] == 1
    assert len(res["files"]) == 1
    assert Path(res["files"]["S0410"]).exists()


def test_download_himawari_slot_all_success_and_logs(tmp_path, mocker):
    dt = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)
    mock_fetch = mocker.patch("src.ingestion.himawari_client.fetch_with_retry")

    content_s4 = b"FAKE_SEGMENT_04"
    content_s5 = b"FAKE_SEGMENT_05"
    mock_fetch.side_effect = [
        Mock(status_code=200, content=content_s4),
        Mock(status_code=200, content=content_s5),
    ]

    res = download_himawari_slot(dt, segments=["S0410", "S0510"], base_dir=str(tmp_path))
    assert res["status"] == "SUCCESS"
    assert res["expected_segments"] == 2
    assert res["downloaded_segments"] == 2
    assert res["bytes_written"] == len(content_s4) + len(content_s5)
    assert Path(res["files"]["S0410"]).read_bytes() == content_s4
    assert Path(res["files"]["S0510"]).read_bytes() == content_s5

    # Check ingestion log written
    log_file = tmp_path / "logs" / "ingestion_log.jsonl"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["source"] == "himawari_hsd"
    assert entry["status"] == "SUCCESS"
    assert entry["expected_segments"] == 2
    assert entry["downloaded_segments"] == 2


def test_download_himawari_slot_all_failed(tmp_path, mocker):
    dt = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)
    mock_fetch = mocker.patch("src.ingestion.himawari_client.fetch_with_retry")
    mock_fetch.side_effect = Exception("S3 Connection failed")

    res = download_himawari_slot(dt, segments=["S0410", "S0510"], base_dir=str(tmp_path))
    assert res["status"] == "FAILED"
    assert res["expected_segments"] == 2
    assert res["downloaded_segments"] == 0
    assert len(res["files"]) == 0
    assert res["bytes_written"] == 0


def test_find_latest_available_slot_probe_head_success(mocker):
    # 2026-11-09 08:24 UTC -> minus 15 min = 08:09 -> rounded down to 10 min = 08:00
    now = datetime(2026, 11, 9, 8, 24, tzinfo=timezone.utc)
    mock_head = mocker.patch("requests.head")
    mock_head.return_value = Mock(status_code=200)

    slot = find_latest_available_slot(now_utc=now, max_lookback_slots=4)
    assert slot == datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)
    assert mock_head.called


def test_find_latest_available_slot_looks_back_when_not_ready(mocker):
    # now is 08:20 -> minus 15 min = 08:05 -> rounded down = 08:00
    now = datetime(2026, 11, 9, 8, 20, tzinfo=timezone.utc)
    mock_head = mocker.patch("requests.head")
    # First slot 08:00 fails (404), second slot 07:50 succeeds (200)
    mock_head.side_effect = [
        Mock(status_code=404),
        Mock(status_code=200),
    ]

    slot = find_latest_available_slot(now_utc=now, max_lookback_slots=4)
    assert slot == datetime(2026, 11, 9, 7, 50, tzinfo=timezone.utc)


def test_find_latest_available_slot_fallback_to_s3_xml_when_probe_fails(mocker):
    now = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)
    # HEAD probes all return 404
    mock_head = mocker.patch("requests.head")
    mock_head.return_value = Mock(status_code=404)

    # Fallback queries S3 XML listing
    xml_sample = """<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <CommonPrefixes><Prefix>AHI-L1b-FLDK/2026/11/08/</Prefix></CommonPrefixes>
        <CommonPrefixes><Prefix>AHI-L1b-FLDK/2026/11/09/</Prefix></CommonPrefixes>
    </ListBucketResult>
    """
    slot_xml_sample = """<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <CommonPrefixes><Prefix>AHI-L1b-FLDK/2026/11/09/0600/</Prefix></CommonPrefixes>
        <CommonPrefixes><Prefix>AHI-L1b-FLDK/2026/11/09/0610/</Prefix></CommonPrefixes>
    </ListBucketResult>
    """
    mock_get = mocker.patch("requests.get")
    mock_get.side_effect = [
        Mock(status_code=200, text=xml_sample),
        Mock(status_code=200, text=slot_xml_sample),
    ]

    slot = find_latest_available_slot(now_utc=now, max_lookback_slots=2)
    assert slot == datetime(2026, 11, 9, 6, 10, tzinfo=timezone.utc)


def test_cleanup_old_raw_segments_preserves_pinned_and_unprocessed(tmp_path):
    hsd_slot = tmp_path / "raw" / "himawari_hsd" / "date=2026-11-01" / "slot=0100"
    hsd_slot.mkdir(parents=True)
    old_file = hsd_slot / "HS_H09_20261101_0100_B13_FLDK_R20_S0410.DAT.bz2"
    old_file.write_bytes(b"12345")

    # Without metrics JSON, it must NOT be deleted
    removed = cleanup_old_raw_segments(base_dir=str(tmp_path), retain_hours=1, pinned_slots=[])
    assert removed == 0
    assert old_file.exists()

    # Once metrics JSON exists for 20261101_0100, it can be cleaned up unless pinned
    metrics_dir = tmp_path / "raw" / "himawari_metrics" / "date=2026-11-01"
    metrics_dir.mkdir(parents=True)
    (metrics_dir / "himawari_metrics_20261101_0100.json").write_text("{}", encoding="utf-8")

    # Pinned -> still preserved
    removed_pinned = cleanup_old_raw_segments(
        base_dir=str(tmp_path), retain_hours=0, pinned_slots=["2026-11-01T01:00:00Z"]
    )
    assert removed_pinned == 0

    # Unpinned -> removed
    removed_unpinned = cleanup_old_raw_segments(
        base_dir=str(tmp_path), retain_hours=0, pinned_slots=[]
    )
    assert removed_unpinned == 1
    assert not old_file.exists()


def test_cleanup_old_raw_segments_preserves_compact_pinned_and_recent_files(tmp_path):
    hsd_slot = tmp_path / "raw" / "himawari_hsd" / "date=2026-11-01" / "slot=0100"
    hsd_slot.mkdir(parents=True)
    old_file = hsd_slot / "HS_H09_20261101_0100_B13_FLDK_R20_S0410.DAT.bz2"
    old_file.write_bytes(b"12345")

    metrics_dir = tmp_path / "raw" / "himawari_metrics" / "date=2026-11-01"
    metrics_dir.mkdir(parents=True)
    (metrics_dir / "himawari_metrics_20261101_0100_batch123.json").write_text("{}", encoding="utf-8")

    # Pinned with compact slot "20261101_0100"
    removed = cleanup_old_raw_segments(
        base_dir=str(tmp_path), retain_hours=0, pinned_slots=["20261101_0100"]
    )
    assert removed == 0
    assert old_file.exists()
