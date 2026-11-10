import bz2
import json
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pytest

from src.ingestion.satpy_processor import (
    compute_dual_box_metrics,
    render_annotated_b13_png,
    process_himawari_slot,
    inject_synthetic_corrupt_test,
    validate_hsd_bz2_file,
    load_segment_grid_with_satpy,
    log_binary_file_error,
)


def test_compute_dual_box_metrics_filters_invalid_and_thresholds():
    lats_1d = np.linspace(15.0439, 17.0439, 21)
    lons_1d = np.linspace(107.1994, 109.1994, 21)
    lons, lats = np.meshgrid(lons_1d, lats_1d)

    bt = np.full_like(lons, 280.0)
    bt[10, 10] = 215.0
    bt[10, 11] = 230.0
    bt[0, 0] = 99.0
    bt[0, 1] = np.nan

    airport_cfg = {
        "icao": "VVDN",
        "name": "Da Nang International Airport",
        "latitude": 16.0439,
        "longitude": 108.1994,
        "himawari_segment": "S0410",
        "outer_half_deg": 1.0,
        "inner_half_deg": 0.25,
    }

    metrics = compute_dual_box_metrics(lons, lats, bt, airport_cfg)
    assert metrics["outer_total_pixels"] == 441
    assert metrics["outer_valid_pixels"] == 439
    assert round(metrics["b13_valid_fraction"], 4) == round(439 / 441, 4)
    assert metrics["inner_bt_min_k"] == 215.0
    assert round(metrics["inner_bt_min_c"], 2) == round(215.0 - 273.15, 2)
    assert metrics["inner_cold_cloud_fraction"] > 0.0
    assert metrics["inner_severe_convection_fraction"] > 0.0


def test_render_annotated_b13_png_creates_image(tmp_path):
    lats_1d = np.linspace(15.0439, 17.0439, 25)
    lons_1d = np.linspace(107.1994, 109.1994, 25)
    lons, lats = np.meshgrid(lons_1d, lats_1d)
    bt = np.full_like(lons, 250.0)

    airport_cfg = {
        "icao": "VVDN",
        "name": "Da Nang International Airport",
        "latitude": 16.0439,
        "longitude": 108.1994,
        "outer_half_deg": 1.0,
        "inner_half_deg": 0.25,
    }
    out_png = tmp_path / "B13_VVDN_20261109_0800Z.png"
    saved = render_annotated_b13_png(
        lons, lats, bt, airport_cfg, "2026-11-09T08:00:00Z", str(out_png)
    )
    assert Path(saved).exists()
    assert Path(saved).stat().st_size > 1000


def test_corrupt_bz2_logs_to_himawari_file_errors(tmp_path):
    res = inject_synthetic_corrupt_test(base_dir=str(tmp_path))
    assert res["is_synthetic_test"] is True
    err_log = tmp_path / "logs" / "himawari_file_errors.jsonl"
    assert err_log.exists()
    row = json.loads(err_log.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert row["is_synthetic_test"] is True
    assert row["segment"] == "S0410"


def test_validate_hsd_bz2_file_checks_integrity(tmp_path):
    # Valid compressed file with >= 1500 bytes of dummy HSD payload
    valid_file = tmp_path / "valid.DAT.bz2"
    with bz2.open(valid_file, "wb") as f:
        f.write(b"\x01" + b"\x00" * 2000)

    # Should not raise
    validate_hsd_bz2_file(valid_file)

    # Truncated decompressed payload
    too_short = tmp_path / "too_short.DAT.bz2"
    with bz2.open(too_short, "wb") as f:
        f.write(b"\x01" + b"\x00" * 100)

    with pytest.raises(ValueError, match="CorruptBZ2OrHSDError"):
        validate_hsd_bz2_file(too_short)

    # Totally corrupt bz2
    corrupt_file = tmp_path / "corrupt.DAT.bz2"
    corrupt_file.write_bytes(b"not a valid bz2 file")
    with pytest.raises(ValueError, match="CorruptBZ2OrHSDError"):
        validate_hsd_bz2_file(corrupt_file)


def test_process_himawari_slot_success(tmp_path):
    airports_cfg = [
        {
            "icao": "VVDN",
            "name": "Da Nang International Airport",
            "latitude": 16.0439,
            "longitude": 108.1994,
            "himawari_segment": "S0410",
            "outer_half_deg": 1.0,
            "inner_half_deg": 0.25,
        },
        {
            "icao": "VVTS",
            "name": "Tan Son Nhat International Airport",
            "latitude": 10.8188,
            "longitude": 106.6520,
            "himawari_segment": "S0510",
            "outer_half_deg": 1.0,
            "inner_half_deg": 0.25,
        },
    ]

    # Create dummy segment files
    raw_hsd_dir = tmp_path / "raw" / "himawari_hsd"
    raw_hsd_dir.mkdir(parents=True, exist_ok=True)
    f_s0410 = raw_hsd_dir / "HS_H09_20261109_0800_B13_FLDK_R20_S0410.DAT.bz2"
    f_s0410.write_bytes(b"dummy")

    segment_files = {
        "S0410": str(f_s0410),
    }

    # Mock load_segment_grid_with_satpy
    def mock_loader(file_path, bbox=None):
        lats_1d = np.linspace(15.0439, 17.0439, 21)
        lons_1d = np.linspace(107.1994, 109.1994, 21)
        lons, lats = np.meshgrid(lons_1d, lats_1d)
        bt = np.full_like(lons, 270.0)
        return lons, lats, bt

    with patch("src.ingestion.satpy_processor.load_segment_grid_with_satpy", side_effect=mock_loader):
        res = process_himawari_slot(
            scan_slot_utc="2026-11-09T08:00:00Z",
            segment_files=segment_files,
            airports_config=airports_cfg,
            base_dir=str(tmp_path),
        )

    assert "metadata" in res
    assert "airports" in res
    vvdn = next(a for a in res["airports"] if a["airport_icao"] == "VVDN")
    assert vvdn["status"] == "AVAILABLE"
    assert vvdn["b13_valid_fraction"] == 1.0
    assert Path(vvdn["png_path"]).exists()

    vvts = next(a for a in res["airports"] if a["airport_icao"] == "VVTS")
    assert vvts["status"] == "MISSING_SEGMENT"
    assert vvts["b13_valid_fraction"] == 0.0

    # Verify metrics envelope is saved via RawEnvelopeStore
    saved_metrics = Path(res["metadata"]["saved_path"])
    assert saved_metrics.exists()
    from src.ingestion.raw_store import RawEnvelopeStore
    store = RawEnvelopeStore(str(tmp_path))
    envelopes = list(store.iter_envelopes("himawari_metrics"))
    assert len(envelopes) == 1
    assert envelopes[0]["metadata"]["scan_slot_utc"] == "2026-11-09T08:00:00Z"


def test_process_himawari_slot_handles_corrupt_segment(tmp_path):
    airports_cfg = [
        {
            "icao": "VVDN",
            "latitude": 16.0439,
            "longitude": 108.1994,
            "himawari_segment": "S0410",
        }
    ]
    corrupt_file = tmp_path / "corrupt_S0410.DAT.bz2"
    corrupt_file.write_bytes(b"bad content")

    segment_files = {"S0410": str(corrupt_file)}

    with patch(
        "src.ingestion.satpy_processor.load_segment_grid_with_satpy",
        side_effect=ValueError("CorruptBZ2OrHSDError: Bad header"),
    ):
        res = process_himawari_slot(
            scan_slot_utc="2026-11-09T08:00:00Z",
            segment_files=segment_files,
            airports_config=airports_cfg,
            base_dir=str(tmp_path),
        )

    vvdn = res["airports"][0]
    assert vvdn["status"] == "CORRUPT_SEGMENT"
    assert vvdn["b13_valid_fraction"] == 0.0

    # Verify error is logged to himawari_file_errors.jsonl
    err_log = tmp_path / "logs" / "himawari_file_errors.jsonl"
    assert err_log.exists()
    row = json.loads(err_log.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert row["segment"] == "S0410"
    assert "CorruptBZ2OrHSDError" in row["error_message"] or "CorruptBZ2OrHSDError" in row["error_type"]
