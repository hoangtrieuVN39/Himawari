"""Tests for shared filesystem helpers."""

import hashlib
from pathlib import Path

import pytest

from src.ingestion.fs_utils import atomic_write_bytes, file_sha256, is_temp_file


@pytest.mark.parametrize(
    "name,expected",
    [
        ("batches-0001.jsonl", False),
        ("metar_x.json", False),
        ("batches-0001.jsonl.tmp", True),
        ("batches-0001.jsonl.tmp.1234_ab12cd", True),
        ("storage_state.json.tmp.99", True),
        ("download.part", True),
        ("plain.png", False),
    ],
)
def test_is_temp_file(name, expected):
    assert is_temp_file(name) is expected


def test_file_sha256_matches_hashlib(tmp_path: Path):
    p = tmp_path / "a.bin"
    p.write_bytes(b"hello" * 1000)
    assert file_sha256(p) == hashlib.sha256(b"hello" * 1000).hexdigest()


def test_atomic_write_bytes_replaces_and_leaves_no_temp(tmp_path: Path):
    dst = tmp_path / "sub" / "out.json"
    atomic_write_bytes(dst, b"one")
    atomic_write_bytes(dst, b"two")
    assert dst.read_bytes() == b"two"
    assert [p.name for p in dst.parent.iterdir()] == ["out.json"]
