"""Tests verifying business parity between Legacy JSON layout and Daily JSONL compact layout."""

import json
from pathlib import Path
import pytest

from src.dashboard.data_loader import _read_parquet_rows
from src.ingestion.raw_store import RawEnvelopeStore
from src.spark.lake_paths import local_table_version


def _populate_fixtures(root: Path, layout: str):
    """Populates identical test envelopes into root using the specified layout."""
    store = RawEnvelopeStore(storage_root=str(root))
    store._initialize_marker(layout)

    metar_env = {
        "metadata": {
            "batch_id": "20261109T080000Z_metar_test01",
            "source": "aviationweather_metar",
            "expected_slot_utc": "2026-11-09T08:00:00Z",
            "fetched_at_utc": "2026-11-09T08:00:00Z",
            "is_backfill": False,
        },
        "payload": [
            {
                "icaoId": "VVDN",
                "obsTime": 1794211200,
                "reportTime": "2026-11-09T08:00:00Z",
                "rawOb": "METAR VVDN 090800Z 09008KT 9999 FEW018 28/24 Q1012 NOSIG",
                "temp": 28.0,
                "dewp": 24.0,
                "visib": "10+",
            },
            {
                "icaoId": "VVNB",
                "obsTime": 1794211200,
                "reportTime": "2026-11-09T08:00:00Z",
                "rawOb": "METAR VVNB 090800Z 12004KT 4000 -RA BKN012 25/23 Q1011 TEMPO 2000 TSRA",
                "temp": 25.0,
                "dewp": 23.0,
                "visib": "4000",
            },
            {
                "icaoId": "VVTS",
                "obsTime": 1794211200,
                "reportTime": "2026-11-09T08:00:00Z",
                "rawOb": "METAR VVTS 090800Z 24010KT 5000 TS BKN015CB 30/26 Q1009 BECMG 3000 +TSRA",
                "temp": 30.0,
                "dewp": 26.0,
                "visib": "5000",
            },
        ],
    }

    forecast_env_vvdn = {
        "metadata": {
            "batch_id": "20261109T080000Z_om_VVDN_test01",
            "source": "open_meteo_forecast",
            "airport_icao": "VVDN",
            "expected_slot_utc": "2026-11-09T08:00:00Z",
            "fetched_at_utc": "2026-11-09T08:00:00Z",
            "is_backfill": False,
        },
        "payload": {
            "hourly": {
                "time": ["2026-11-09T09:00"],
                "precipitation": [0.0],
                "rain": [0.0],
                "showers": [0.0],
                "weather_code": [1],
                "visibility": [10000.0],
            }
        },
    }

    himawari_env = {
        "metadata": {
            "batch_id": "20261109T080000Z_hima_test01",
            "source": "himawari_metrics",
            "scan_slot_utc": "2026-11-09T08:00:00Z",
            "processed_at_utc": "2026-11-09T08:00:00Z",
            "is_backfill": False,
        },
        "airports": [
            {
                "airport_icao": "VVDN",
                "status": "AVAILABLE",
                "scan_time_utc": "2026-11-09T08:00:00Z",
                "fetched_at_utc": "2026-11-09T08:00:00Z",
                "b13_valid_fraction": 1.0,
                "inner_bt_min_k": 285.0,
                "outer_bt_min_k": 280.0,
                "outer_cold_cloud_fraction": 0.0,
                "outer_severe_convection_fraction": 0.0,
                "png_rel_path": "data/processed/himawari_images/VVDN/20261109_0800_VVDN.png",
            }
        ],
    }

    store.write("metar", "2026-11-09", metar_env)
    store.write("open_meteo", "2026-11-09", forecast_env_vvdn)
    store.write("himawari_metrics", "2026-11-09", himawari_env)


def test_python_reader_strict_mode_rejects_malformed_line(tmp_path: Path):
    """Verify that RawEnvelopeStore.iter_envelopes raises CorruptShardError on malformed JSON in strict mode."""
    from src.ingestion.raw_store import CorruptShardError
    compact_root = tmp_path / "compact"
    store = RawEnvelopeStore(storage_root=str(compact_root))
    store._initialize_marker("daily_jsonl")

    # Manually write 3 lines to shard: line 1 good, line 2 bad, line 3 good
    date_dir = compact_root / "raw_compact" / "metar" / "date=2026-11-09"
    date_dir.mkdir(parents=True, exist_ok=True)
    shard_file = date_dir / "batches-0001.jsonl"
    line1 = json.dumps({"metadata": {"batch_id": "b1"}})
    line2 = "THIS IS NOT VALID JSON {{"
    line3 = json.dumps({"metadata": {"batch_id": "b3"}})
    shard_file.write_text(f"{line1}\n{line2}\n{line3}\n", encoding="utf-8")

    with pytest.raises(CorruptShardError):
        list(store.iter_envelopes("metar", strict=True))

    # In non-strict mode, returns the 2 valid envelopes and skips malformed line
    envelopes = list(store.iter_envelopes("metar", strict=False))
    assert len(envelopes) == 2
    assert envelopes[0]["metadata"]["batch_id"] == "b1"
    assert envelopes[1]["metadata"]["batch_id"] == "b3"


def _run_pipeline(root: Path, spark):
    from src.spark.run_pipeline import run_spark_pipeline

    return run_spark_pipeline(
        storage_root=str(root),
        review_slots=["2026-11-09T08:00:00Z"],
        mode="replay",
        spark=spark,
    )


def _load_decision_rows(root: Path) -> list[dict]:
    rows = _read_parquet_rows(local_table_version(f"{root}/lake", "decision_table"))
    return sorted(rows, key=lambda r: r["airport_icao"])


def test_spark_pipeline_parity_between_layouts(tmp_path: Path, spark):
    """TC-SPARK-01: legacy and compact layouts yield identical business decisions."""
    legacy_root = tmp_path / "legacy"
    compact_root = tmp_path / "compact"
    _populate_fixtures(legacy_root, "legacy_files")
    _populate_fixtures(compact_root, "daily_jsonl")

    res_legacy = _run_pipeline(legacy_root, spark)
    res_compact = _run_pipeline(compact_root, spark)

    assert res_legacy["decision_rows_count"] == res_compact["decision_rows_count"] > 0
    for lr, cr in zip(_load_decision_rows(legacy_root), _load_decision_rows(compact_root)):
        for key in ("airport_icao", "priority_level", "priority_label", "reason_vi",
                    "observed_at_utc", "accepted_first_review_airports"):
            assert lr[key] == cr[key], key


def test_spark_pipeline_quarantines_corrupt_jsonl_line(tmp_path: Path, spark):
    """TC-SPARK-02: a malformed line in a compact shard is quarantined, valid lines still processed."""
    root = tmp_path / "compact"
    _populate_fixtures(root, "daily_jsonl")
    shard = root / "raw_compact" / "metar" / "date=2026-11-09" / "batches-0001.jsonl"
    shard.write_text(shard.read_text(encoding="utf-8") + "{NOT VALID JSON\n", encoding="utf-8")

    result = _run_pipeline(root, spark)

    assert result["decision_rows_count"] > 0
    quarantined = _read_parquet_rows(local_table_version(f"{root}/lake", "quarantine"))
    assert any("SCHEMA_VIOLATION" in (q.get("error_codes") or []) and q["source"] == "metar" for q in quarantined)


def test_spark_pipeline_does_not_fall_back_to_legacy_when_compact_active(tmp_path: Path, spark):
    """Single-layout rule: legacy raw/*.json must be ignored when daily_jsonl is the active layout."""
    root = tmp_path / "compact"
    _populate_fixtures(root, "daily_jsonl")
    # A legacy METAR file that must NOT be read
    legacy_dir = root / "raw" / "metar" / "date=2026-11-09"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "metar_legacy_only.json").write_text(
        json.dumps({
            "metadata": {"batch_id": "legacy_only", "fetched_at_utc": "2026-11-09T08:00:00Z", "is_backfill": False},
            "payload": [{"icaoId": "VVDN", "reportTime": "2026-11-09T08:00:00Z",
                         "rawOb": "METAR VVDN 090800Z 09008KT 9999 FEW018 28/24 Q1012"}],
        }),
        encoding="utf-8",
    )
    # Remove compact METAR so a fallback would be the only METAR source
    for shard in (root / "raw_compact" / "metar").rglob("*.jsonl"):
        shard.unlink()

    result = _run_pipeline(root, spark)
    metar_stats = next(s for s in result["reconciliation"]["sources"] if s["source"] == "metar")
    assert metar_stats["rows_read"] == 0
