import bz2
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Union

if "MPLCONFIGDIR" not in os.environ:
    os.environ["MPLCONFIGDIR"] = tempfile.gettempdir()
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np

from src.ingestion.storage import save_raw_envelope, append_ingestion_log

# Physical constants and thresholds
MIN_VALID_BT_K = 150.0
MAX_VALID_BT_K = 340.0
COLD_CLOUD_THRESHOLD_K = 235.0          # -38.15°C
SEVERE_CONVECTION_THRESHOLD_K = 220.0   # -53.15°C
KELVIN_TO_CELSIUS_OFFSET = 273.15
MANDATORY_ATTRIBUTION = (
    "Processed Product | Source: JMA/NOAA Himawari-9 B13 via AWS Open Data | "
    "Cropped & Annotated by Group"
)


def validate_hsd_bz2_file(file_path: Union[str, Path]) -> None:
    """
    Decompresses and validates non-empty HSD header (>= 1500 bytes).
    Raises ValueError("CorruptBZ2OrHSDError: ...") if file cannot be decompressed
    or header is truncated.
    """
    path = Path(file_path)
    if not path.exists():
        raise ValueError(f"CorruptBZ2OrHSDError: File not found: {file_path}")

    try:
        with bz2.open(path, "rb") as f:
            header_bytes = f.read(1600)
    except Exception as e:
        raise ValueError(f"CorruptBZ2OrHSDError: Decompression failed for {file_path}: {e}") from e

    if len(header_bytes) < 1500:
        raise ValueError(
            f"CorruptBZ2OrHSDError: Header truncated ({len(header_bytes)} bytes < 1500) in {file_path}"
        )


def load_segment_grid_with_satpy(
    file_path: Union[str, Path, List[Union[str, Path]]],
    bbox: Optional[Tuple[float, float, float, float]] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Loads Himawari-9 AHI HSD segment(s) using Satpy.
    Validates BZ2/HSD integrity first, then loads B13 and crops to bbox if provided.
    Returns (lons, lats, bt_kelvin) as 2D numpy arrays.
    """
    if isinstance(file_path, (list, tuple)):
        for f in file_path:
            validate_hsd_bz2_file(f)
        filenames = [str(f) for f in file_path]
    else:
        validate_hsd_bz2_file(file_path)
        filenames = [str(file_path)]

    from satpy import Scene

    scn = Scene(filenames=filenames, reader="ahi_hsd")
    scn.load(["B13"])

    if bbox is not None:
        # bbox order in satpy: (ll_bbox=(lon_min, lat_min, lon_max, lat_max))
        scn = scn.crop(ll_bbox=bbox)

    da = scn["B13"]
    area = da.attrs.get("area")
    if area is not None and hasattr(area, "get_lonlats"):
        lons, lats = area.get_lonlats()
    elif "longitude" in da.coords and "latitude" in da.coords:
        lons = da.coords["longitude"].values
        lats = da.coords["latitude"].values
    else:
        raise ValueError(f"Cannot extract coordinates from dataset area in {filenames}")

    bt_kelvin = da.values
    return np.asarray(lons, dtype=float), np.asarray(lats, dtype=float), np.asarray(bt_kelvin, dtype=float)


def compute_dual_box_metrics(
    lons: np.ndarray,
    lats: np.ndarray,
    bt_kelvin: np.ndarray,
    airport_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Computes dual-box brightness temperature statistics for the given airport configuration.
    Outer box (default +/-1.0 deg) and Inner box (default +/-0.25 deg).
    Filters pixels outside physical valid range [150.0 K, 340.0 K].
    """
    lat0 = float(airport_cfg["latitude"])
    lon0 = float(airport_cfg["longitude"])
    outer_half = float(airport_cfg.get("outer_half_deg", 1.0))
    inner_half = float(airport_cfg.get("inner_half_deg", 0.25))

    lons_arr = np.asarray(lons, dtype=float)
    lats_arr = np.asarray(lats, dtype=float)
    bt_arr = np.asarray(bt_kelvin, dtype=float)

    if lons_arr.ndim == 1 and lats_arr.ndim == 1:
        lons_grid, lats_grid = np.meshgrid(lons_arr, lats_arr)
    else:
        lons_grid, lats_grid = lons_arr, lats_arr

    outer_mask = (np.abs(lats_grid - lat0) <= outer_half) & (np.abs(lons_grid - lon0) <= outer_half)
    inner_mask = (np.abs(lats_grid - lat0) <= inner_half) & (np.abs(lons_grid - lon0) <= inner_half)
    valid_mask = np.isfinite(bt_arr) & (bt_arr >= MIN_VALID_BT_K) & (bt_arr <= MAX_VALID_BT_K)

    outer_total_pixels = int(np.sum(outer_mask))
    outer_valid_mask = outer_mask & valid_mask
    outer_valid_pixels = int(np.sum(outer_valid_mask))
    b13_valid_fraction = (
        float(outer_valid_pixels / outer_total_pixels) if outer_total_pixels > 0 else 0.0
    )

    if outer_valid_pixels > 0:
        outer_vals = bt_arr[outer_valid_mask]
        outer_bt_min_k = float(np.min(outer_vals))
        outer_bt_min_c = float(outer_bt_min_k - KELVIN_TO_CELSIUS_OFFSET)
        outer_bt_mean_k = float(np.mean(outer_vals))
        outer_bt_mean_c = float(outer_bt_mean_k - KELVIN_TO_CELSIUS_OFFSET)
        outer_cold_cloud_fraction = float(
            np.sum(outer_vals < COLD_CLOUD_THRESHOLD_K) / outer_valid_pixels
        )
        outer_severe_convection_fraction = float(
            np.sum(outer_vals < SEVERE_CONVECTION_THRESHOLD_K) / outer_valid_pixels
        )
    else:
        outer_bt_min_k = None
        outer_bt_min_c = None
        outer_bt_mean_k = None
        outer_bt_mean_c = None
        outer_cold_cloud_fraction = 0.0
        outer_severe_convection_fraction = 0.0

    inner_total_pixels = int(np.sum(inner_mask))
    inner_valid_mask = inner_mask & valid_mask
    inner_valid_pixels = int(np.sum(inner_valid_mask))
    inner_valid_fraction = (
        float(inner_valid_pixels / inner_total_pixels) if inner_total_pixels > 0 else 0.0
    )

    if inner_valid_pixels > 0:
        inner_vals = bt_arr[inner_valid_mask]
        inner_bt_min_k = float(np.min(inner_vals))
        inner_bt_min_c = float(inner_bt_min_k - KELVIN_TO_CELSIUS_OFFSET)
        inner_bt_mean_k = float(np.mean(inner_vals))
        inner_bt_mean_c = float(inner_bt_mean_k - KELVIN_TO_CELSIUS_OFFSET)
        inner_cold_cloud_fraction = float(
            np.sum(inner_vals < COLD_CLOUD_THRESHOLD_K) / inner_valid_pixels
        )
        inner_severe_convection_fraction = float(
            np.sum(inner_vals < SEVERE_CONVECTION_THRESHOLD_K) / inner_valid_pixels
        )
    else:
        inner_bt_min_k = None
        inner_bt_min_c = None
        inner_bt_mean_k = None
        inner_bt_mean_c = None
        inner_cold_cloud_fraction = 0.0
        inner_severe_convection_fraction = 0.0

    return {
        "outer_total_pixels": outer_total_pixels,
        "outer_valid_pixels": outer_valid_pixels,
        "b13_valid_fraction": b13_valid_fraction,
        "outer_bt_min_k": outer_bt_min_k,
        "outer_bt_min_c": outer_bt_min_c,
        "outer_bt_mean_k": outer_bt_mean_k,
        "outer_bt_mean_c": outer_bt_mean_c,
        "outer_cold_cloud_fraction": outer_cold_cloud_fraction,
        "outer_severe_convection_fraction": outer_severe_convection_fraction,
        "inner_total_pixels": inner_total_pixels,
        "inner_valid_pixels": inner_valid_pixels,
        "inner_valid_fraction": inner_valid_fraction,
        "inner_bt_min_k": inner_bt_min_k,
        "inner_bt_min_c": inner_bt_min_c,
        "inner_bt_mean_k": inner_bt_mean_k,
        "inner_bt_mean_c": inner_bt_mean_c,
        "inner_cold_cloud_fraction": inner_cold_cloud_fraction,
        "inner_severe_convection_fraction": inner_severe_convection_fraction,
    }


def render_annotated_b13_png(
    lons: np.ndarray,
    lats: np.ndarray,
    bt_kelvin: np.ndarray,
    airport_cfg: Dict[str, Any],
    scan_slot_utc: str,
    output_path: str,
) -> str:
    """
    Renders infrared brightness temperature in Celsius using matplotlib (Agg backend).
    Marks airport center (+), draws +/-inner_half_deg dashed box, colorbar, and
    mandatory JMA/NOAA attribution footer. Writes atomically via .tmp.png.
    """
    lat0 = float(airport_cfg["latitude"])
    lon0 = float(airport_cfg["longitude"])
    outer_half = float(airport_cfg.get("outer_half_deg", 1.0))
    inner_half = float(airport_cfg.get("inner_half_deg", 0.25))
    icao = airport_cfg.get("icao", "AIRPORT")

    lons_arr = np.asarray(lons, dtype=float)
    lats_arr = np.asarray(lats, dtype=float)
    bt_arr = np.asarray(bt_kelvin, dtype=float)

    if lons_arr.ndim == 1 and lats_arr.ndim == 1:
        lons_grid, lats_grid = np.meshgrid(lons_arr, lats_arr)
    else:
        lons_grid, lats_grid = lons_arr, lats_arr

    valid_mask = np.isfinite(bt_arr) & (bt_arr >= MIN_VALID_BT_K) & (bt_arr <= MAX_VALID_BT_K)
    bt_celsius = bt_arr - KELVIN_TO_CELSIUS_OFFSET
    masked_bt_c = np.ma.masked_where(~valid_mask, bt_celsius)

    fig, ax = plt.subplots(figsize=(7, 6), dpi=150)

    # Plot satellite grid
    pcm = ax.pcolormesh(
        lons_grid,
        lats_grid,
        masked_bt_c,
        cmap="inferno_r",
        shading="auto",
        vmin=-80.0,
        vmax=40.0,
    )

    # Colorbar
    cbar = fig.colorbar(pcm, ax=ax, orientation="vertical", pad=0.03)
    cbar.set_label("Brightness Temp (°C)")

    # Mark airport center
    ax.scatter(lon0, lat0, color="red", marker="+", s=140, linewidths=2.5, zorder=5)
    ax.text(
        lon0 + 0.05,
        lat0 + 0.05,
        icao,
        color="red",
        fontsize=10,
        fontweight="bold",
        zorder=6,
    )

    # Inner box rectangle (+/- inner_half_deg)
    rect = patches.Rectangle(
        (lon0 - inner_half, lat0 - inner_half),
        2 * inner_half,
        2 * inner_half,
        linewidth=1.5,
        edgecolor="cyan",
        facecolor="none",
        linestyle="--",
        zorder=4,
    )
    ax.add_patch(rect)

    # Limits and labels
    ax.set_xlim(lon0 - outer_half, lon0 + outer_half)
    ax.set_ylim(lat0 - outer_half, lat0 + outer_half)
    ax.set_title(f"Himawari-9 B13 | {icao} | {scan_slot_utc}", fontsize=11, fontweight="bold")
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")

    # Mandatory attribution footer
    fig.text(
        0.5,
        0.01,
        MANDATORY_ATTRIBUTION,
        ha="center",
        va="bottom",
        fontsize=7,
        color="dimgray",
    )

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = out_file.parent / f"{out_file.name}.tmp.png"

    fig.savefig(str(tmp_file), bbox_inches="tight")
    plt.close(fig)

    os.replace(tmp_file, out_file)
    return str(out_file)


def log_binary_file_error(
    scan_slot_utc: str,
    segment: str,
    file_path: str,
    error: Union[Exception, str],
    is_synthetic_test: bool = False,
    base_dir: str = "data",
) -> Dict[str, Any]:
    """
    Logs corrupted or unreadable Himawari binary file errors to
    <base_dir>/logs/himawari_file_errors.jsonl.
    """
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    err_type = type(error).__name__ if isinstance(error, Exception) else "CorruptBZ2OrHSDError"
    err_msg = str(error)
    file_size = 0
    try:
        p = Path(file_path)
        if p.exists():
            file_size = p.stat().st_size
    except Exception:
        file_size = 0

    entry = {
        "timestamp_utc": now_iso,
        "scan_slot_utc": scan_slot_utc,
        "segment": segment,
        "file_path": str(file_path),
        "file_size_bytes": file_size,
        "error_type": err_type,
        "error_message": err_msg,
        "is_synthetic_test": is_synthetic_test,
    }

    log_path = str(Path(base_dir) / "logs" / "himawari_file_errors.jsonl")
    append_ingestion_log(entry, log_path=log_path)
    return entry


def inject_synthetic_corrupt_test(base_dir: str = "data") -> Dict[str, Any]:
    """
    Creates a small truncated .DAT.bz2 file in
    <base_dir>/raw/himawari_hsd/synthetic_test/HS_H09_20261109_0800_B13_FLDK_R20_S0410.DAT.bz2,
    validates it to trigger corruption error, logs to himawari_file_errors.jsonl with
    is_synthetic_test=True, and returns the error entry.
    """
    synthetic_dir = Path(base_dir) / "raw" / "himawari_hsd" / "synthetic_test"
    synthetic_dir.mkdir(parents=True, exist_ok=True)
    file_name = "HS_H09_20261109_0800_B13_FLDK_R20_S0410.DAT.bz2"
    file_path = synthetic_dir / file_name

    # Write truncated bz2 payload
    with open(file_path, "wb") as f:
        f.write(b"BZh91AY&SY_SYNTHETIC_CORRUPT_TEST_TRUNCATED")

    scan_slot_utc = "2026-11-09T08:00:00Z"
    segment = "S0410"

    try:
        validate_hsd_bz2_file(file_path)
        err = ValueError(f"CorruptBZ2OrHSDError: File did not raise corruption error: {file_path}")
    except Exception as e:
        err = e

    return log_binary_file_error(
        scan_slot_utc=scan_slot_utc,
        segment=segment,
        file_path=str(file_path),
        error=err,
        is_synthetic_test=True,
        base_dir=base_dir,
    )


def process_himawari_slot(
    scan_slot_utc: str,
    segment_files: Dict[str, str],
    airports_config: Union[Dict[str, Any], List[Dict[str, Any]]],
    base_dir: str = "data",
    is_backfill: bool = False,
) -> Dict[str, Any]:
    """
    Processes Himawari-9 B13 segment files for all airports:
    1. Validates and loads the segment grid around each airport.
    2. Computes dual-box statistics and filters invalid pixels.
    3. Renders annotated PNG to <base_dir>/processed/himawari_png/...
    4. Handles missing or corrupt segments gracefully.
    5. Saves raw metrics envelope to <base_dir>/raw/himawari_metrics/date=YYYY-MM-DD/.
    """
    if isinstance(airports_config, dict):
        if "airports" in airports_config and isinstance(airports_config["airports"], list):
            airports_list = airports_config["airports"]
        else:
            airports_list = list(airports_config.values())
    elif isinstance(airports_config, list):
        airports_list = airports_config
    else:
        airports_list = []

    # Parse scan_slot_utc
    clean_slot = scan_slot_utc.replace("Z", "+00:00")
    try:
        scan_dt = datetime.fromisoformat(clean_slot)
    except ValueError:
        scan_dt = datetime.strptime(scan_slot_utc[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

    if scan_dt.tzinfo is None:
        scan_dt = scan_dt.replace(tzinfo=timezone.utc)
    else:
        scan_dt = scan_dt.astimezone(timezone.utc)

    date_str = scan_dt.strftime("%Y-%m-%d")
    compact_slot = scan_dt.strftime("%Y%m%d_%H%M")
    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    batch_id = f"{now_utc.strftime('%Y%m%dT%H%M%SZ')}_himawari_metrics_{uuid.uuid4().hex[:6]}"

    airport_results: List[Dict[str, Any]] = []

    for airport in airports_list:
        icao = airport.get("icao", "UNKNOWN")
        segment = airport.get("himawari_segment", "S0410")
        lat0 = float(airport["latitude"])
        lon0 = float(airport["longitude"])
        outer_half = float(airport.get("outer_half_deg", 1.0))
        bbox = (lon0 - outer_half, lat0 - outer_half, lon0 + outer_half, lat0 + outer_half)

        if segment in segment_files:
            raw_file = segment_files[segment]
            # When multiple segments are downloaded, pass all available segments to Satpy
            # so boundary airports (e.g. VVTS on S0410/S0510 boundary) receive seamless full coverage
            files_to_load = list(segment_files.values()) if len(segment_files) > 1 else raw_file
            try:
                try:
                    lons, lats, bt_kelvin = load_segment_grid_with_satpy(files_to_load, bbox=bbox)
                except Exception:
                    lons, lats, bt_kelvin = load_segment_grid_with_satpy(raw_file, bbox=bbox)
                metrics = compute_dual_box_metrics(lons, lats, bt_kelvin, airport)

                png_dir = (
                    Path(base_dir)
                    / "processed"
                    / "himawari_png"
                    / f"airport={icao}"
                    / f"date={date_str}"
                )
                png_filename = f"B13_{icao}_{compact_slot}Z.png"
                png_path = png_dir / png_filename

                render_annotated_b13_png(
                    lons=lons,
                    lats=lats,
                    bt_kelvin=bt_kelvin,
                    airport_cfg=airport,
                    scan_slot_utc=scan_slot_utc,
                    output_path=str(png_path),
                )

                try:
                    png_rel_path = str(png_path.relative_to(base_dir)).replace("\\", "/")
                except ValueError:
                    png_rel_path = str(png_path).replace("\\", "/")

                record = {
                    "airport_icao": icao,
                    "segment_code": segment,
                    "scan_time_utc": scan_slot_utc,
                    "fetched_at_utc": now_iso,
                    "status": "AVAILABLE",
                    "raw_file": str(raw_file),
                    "png_rel_path": png_rel_path,
                    "png_path": str(png_path),
                    **metrics,
                }
                airport_results.append(record)

            except Exception as e:
                log_binary_file_error(
                    scan_slot_utc=scan_slot_utc,
                    segment=segment,
                    file_path=str(raw_file),
                    error=e,
                    is_synthetic_test=False,
                    base_dir=base_dir,
                )
                corrupt_record = {
                    "airport_icao": icao,
                    "segment_code": segment,
                    "scan_time_utc": scan_slot_utc,
                    "fetched_at_utc": now_iso,
                    "status": "CORRUPT_SEGMENT",
                    "raw_file": str(raw_file),
                    "png_rel_path": None,
                    "png_path": None,
                    "outer_total_pixels": 0,
                    "outer_valid_pixels": 0,
                    "b13_valid_fraction": 0.0,
                    "outer_bt_min_k": None,
                    "outer_bt_min_c": None,
                    "outer_bt_mean_k": None,
                    "outer_bt_mean_c": None,
                    "outer_cold_cloud_fraction": 0.0,
                    "outer_severe_convection_fraction": 0.0,
                    "inner_total_pixels": 0,
                    "inner_valid_pixels": 0,
                    "inner_valid_fraction": 0.0,
                    "inner_bt_min_k": None,
                    "inner_bt_min_c": None,
                    "inner_bt_mean_k": None,
                    "inner_bt_mean_c": None,
                    "inner_cold_cloud_fraction": 0.0,
                    "inner_severe_convection_fraction": 0.0,
                }
                airport_results.append(corrupt_record)
        else:
            missing_record = {
                "airport_icao": icao,
                "segment_code": segment,
                "scan_time_utc": scan_slot_utc,
                "fetched_at_utc": now_iso,
                "status": "MISSING_SEGMENT",
                "raw_file": None,
                "png_rel_path": None,
                "png_path": None,
                "outer_total_pixels": 0,
                "outer_valid_pixels": 0,
                "b13_valid_fraction": 0.0,
                "outer_bt_min_k": None,
                "outer_bt_min_c": None,
                "outer_bt_mean_k": None,
                "outer_bt_mean_c": None,
                "outer_cold_cloud_fraction": 0.0,
                "outer_severe_convection_fraction": 0.0,
                "inner_total_pixels": 0,
                "inner_valid_pixels": 0,
                "inner_valid_fraction": 0.0,
                "inner_bt_min_k": None,
                "inner_bt_min_c": None,
                "inner_bt_mean_k": None,
                "inner_bt_mean_c": None,
                "inner_cold_cloud_fraction": 0.0,
                "inner_severe_convection_fraction": 0.0,
            }
            airport_results.append(missing_record)

    envelope = {
        "metadata": {
            "batch_id": batch_id,
            "source": "himawari_metrics",
            "scan_slot_utc": scan_slot_utc,
            "processed_at_utc": now_iso,
            "is_backfill": is_backfill,
            "attribution": MANDATORY_ATTRIBUTION,
            "airport_count": len(airports_list),
        },
        "airports": airport_results,
    }

    metrics_filename = f"himawari_metrics_{compact_slot}_{batch_id}.json"
    raw_dir = str(Path(base_dir) / "raw")
    saved_path = save_raw_envelope(
        source="himawari_metrics",
        date_str=date_str,
        filename=metrics_filename,
        envelope=envelope,
        base_dir=raw_dir,
    )
    envelope["metadata"]["saved_path"] = saved_path

    return envelope
