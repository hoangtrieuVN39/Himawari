"""Submission sample: live Bronze only, newest lake versions, no credentials."""

import json
import zipfile
from pathlib import Path

import pytest

from scripts.package_sample import SecretFoundError, build_sample
from src.ingestion.raw_store import RawEnvelopeStore
from tests.dashboard.lake_fixtures import write_lake_version


def _metar(store, batch, backfill):
    store.write("metar", "2026-11-09", {
        "metadata": {"batch_id": batch, "fetched_at_utc": "2026-11-09T07:01:00Z", "is_backfill": backfill},
        "payload": [],
    })


def test_sample_keeps_live_envelopes_and_current_lake_only(tmp_path: Path):
    root = tmp_path / "data"
    store = RawEnvelopeStore(storage_root=str(root))
    _metar(store, "live1", False)
    _metar(store, "backfill1", True)
    write_lake_version(root, "decision_table", [{"airport_icao": "VVDN"}], run_id="20261109T070000000000Z")
    write_lake_version(root, "decision_table", [{"airport_icao": "VVNB"}], run_id="20261109T080000000000Z")
    write_lake_version(root, "metar_observations", [{"airport_icao": "VVNB", "is_backfill": True}])

    manifest = build_sample(str(root), str(tmp_path / "out.zip"), ["2026-11-09"])

    with zipfile.ZipFile(tmp_path / "out.zip") as zf:
        names = zf.namelist()
        shard = zf.read("data/raw_compact/metar/date=2026-11-09/batches-0001.jsonl").decode("utf-8")
    assert [json.loads(l)["metadata"]["batch_id"] for l in shard.splitlines()] == ["live1"]
    assert manifest["backfill_envelopes_dropped"] == 1
    assert manifest["lake_versions"]["decision_table"] == "run_id=20261109T080000000000Z"
    assert not any("run_id=20261109T070000000000Z" in n for n in names)
    assert not any("silver/metar_observations" in n for n in names)  # holds back-filled rows
    assert "data/storage_state.json" in names and "MANIFEST.json" in names


def test_sample_refuses_files_that_look_like_credentials(tmp_path: Path):
    root = tmp_path / "data"
    (root / "logs").mkdir(parents=True)
    (root / "logs" / "ingestion_log.jsonl").write_text('{"aws_secret_access_key": "x"}\n', encoding="utf-8")
    with pytest.raises(SecretFoundError):
        build_sample(str(root), str(tmp_path / "out.zip"), ["2026-11-09"])
