# Module 2 (Hadoop HDFS Dual-Mode & Himawari-9 B13 Satpy Processor) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate a single-node Hadoop HDFS cluster (Dual-Mode local + WebHDFS sync with throughput benchmarking) and build the Himawari-9 Band 13 (`S0410`, `S0510`) satellite downloader and `Satpy` dual-box processor ($\pm 1.0^\circ$ outer context & $\pm 0.25^\circ$ inner station) with physical threshold validation, annotated PNG generation, and binary file corruption logging.

**Architecture:** The system fetches public Himawari-9 HSD `.DAT.bz2` segments (`S0410` covering `VVDN`/`VVNB`, `S0510` covering `VVTS`) from AWS S3 (`https://noaa-himawari9.s3.amazonaws.com`), stores them atomically in `data/raw/himawari_hsd/`, and processes them through `Satpy` (`ahi_hsd` reader). Extracted cloud metrics (`b13_valid_fraction`, inner/outer brightness temperatures, convective fractions $<235\text{ K}$ and $<220\text{ K}$) are written as JSON envelopes for PySpark alongside annotated PNG previews, while `WebHDFSClient` dual-writes or batch-syncs all layers to Hadoop HDFS (`namenode:9870`).

**Tech Stack:** Python 3.11 (`uv`), Apache Hadoop 3 (`apache/hadoop:3` in Docker Compose), `satpy`, `pyresample`, `numpy`, `xarray`, `matplotlib`, `requests`, `pytest`.

**Spec:** `docs/superpowers/specs/2026-11-09-module2-himawari-satpy-design.md`

## Global Constraints

- Python version: 3.11+
- Himawari-9 S3 Bucket URL: `https://noaa-himawari9.s3.amazonaws.com`
- Required Band & Segments: Band `B13`, segments `S0410` (for `VVDN`, `VVNB`) and `S0510` (for `VVTS`)
- Dual-Box Geometry: `outer_half_deg = 1.0` ($\pm 1.0^\circ$), `inner_half_deg = 0.25` ($\pm 0.25^\circ$)
- Physical Brightness Temperature valid range: $[150.0\text{ K}, 340.0\text{ K}]$ ($[-123.15^\circ\text{C}, +66.85^\circ\text{C}]$)
- Cold convective cloud threshold: $T_b < 235.0\text{ K}$ ($-38.15^\circ\text{C}$)
- Severe thunderstorm cloud-top threshold: $T_b < 220.0\text{ K}$ ($-53.15^\circ\text{C}$)
- Mandatory PNG attribution text: `"Processed Product | Source: JMA/NOAA Himawari-9 B13 via AWS Open Data | Cropped & Annotated by Group"`
- Binary file error log: `data/logs/himawari_file_errors.jsonl`
- HDFS sync benchmark log: `data/logs/hdfs_sync_log.jsonl`
- Hadoop NameNode ports: `9870` (WebHDFS/UI), `9000` (IPC `hdfs://namenode:9000`)

## Review Focus

1. Corrupted or truncated `.DAT.bz2` binary segment downloaded from S3 -> Caught before/during `Satpy` load, logged to `data/logs/himawari_file_errors.jsonl` with `error_type` and `file_size_bytes` without crashing the pipeline.
2. Out-of-range sensor noise pixels ($T_b < 150\text{ K}$ or $T_b > 340\text{ K}$ or `NaN`) -> Excluded from valid pixels, reducing `b13_valid_fraction` accurately and never skewing `bt_min_k` or `bt_mean_k`.
3. Partial scan on AWS S3 (e.g., `S0410` uploaded but `S0510` delayed) -> Logged as `status="PARTIAL"` with `expected_segments=2, downloaded_segments=1`, still processing `VVDN` and `VVNB` from `S0410` while marking `VVTS` satellite evidence as missing.
4. Hadoop HDFS containers stopped while running on battery/low RAM -> `WebHDFSClient` detects offline state within `1.5s`, logs `SKIPPED_HDFS_OFFLINE`, and preserves local files for later `--sync-hdfs`.
5. Disk exhaustion during 7-day continuous run -> `cleanup_old_raw_segments` removes `.DAT.bz2` older than `retain_hours` only if metrics JSON exists and the slot is not in `pinned_slots`.

---

### Task 0: Update Project Proposal Document (`docs/ke-hoach-nhom.md`)

**Files:**
- Modify: `docs/ke-hoach-nhom.md`

**Interfaces:**
- Consumes: Architectural decisions from `docs/superpowers/specs/2026-11-09-module2-himawari-satpy-design.md`
- Produces: Updated Sections 2, 3, 4, and 6 in `docs/ke-hoach-nhom.md` reflecting Dual-Mode Storage (Local + Hadoop HDFS in Docker + Parquet), Himawari-9 B13 segments (`S0410`, `S0510`), Dual-Box geometry ($\pm 1.0^\circ$ & $\pm 0.25^\circ$), physical thresholds ($150\text{ K}-340\text{ K}$, $<235\text{ K}$, $<220\text{ K}$), and retention policy.

- [ ] **Step 1: Update Section 2 (Dữ liệu, 5V và quản trị) in `docs/ke-hoach-nhom.md`**

Specify segments `S0410` (covering `VVDN`, `VVNB`) and `S0510` (covering `VVTS`), Dual-Box cropping ($\pm 1.0^\circ$ outer context box and $\pm 0.25^\circ$ inner station box), raw `.DAT.bz2` retention policy (keeping pinned replay windows and cleaning up processed segments older than 24h), and expand the Data Dictionary with `b13_valid_fraction`, `inner_bt_min_c`, `inner_cold_cloud_fraction` ($T_b < 235\text{ K}$), and `inner_severe_convection_fraction` ($T_b < 220\text{ K}$).

- [ ] **Step 2: Update Section 3 (Kiến trúc và ma trận lưu trữ — CLO2) in `docs/ke-hoach-nhom.md`**

Update the processing flow and Storage Matrix table to include **Dual-Mode: Kho tệp cục bộ + Hadoop HDFS (Single-Node Docker `namenode:9870` / `hdfs://namenode:9000`) + Parquet** compared against **PostgreSQL**, detailing Auto Dual-Write + `sync_to_hdfs` and empirical HDFS throughput measurement (`hdfs_sync_log.jsonl`).

- [ ] **Step 3: Update Section 4 & Section 6 (Quy tắc ngưỡng B13, Kiểm thử tệp hỏng & Quản lý rủi ro) in `docs/ke-hoach-nhom.md`**

Document the physical validity range $[150\text{ K}, 340\text{ K}]$, convective thresholds ($<235\text{ K}$ and $<220\text{ K}$), corrupted binary file log (`himawari_file_errors.jsonl` with `is_synthetic_test`), and Docker Compose environment reproducibility.

- [ ] **Step 4: Verify markdown formatting and commit**

```bash
git add "docs/ke-hoach-nhom.md"
git commit -m "docs(plan): update project proposal with Hadoop HDFS dual-mode and Himawari-9 B13 specs"
```

---

### Task 1: Airport Satellite Config & Dependencies Setup

**Files:**
- Modify: `config/airports.json`
- Modify: `requirements.txt`
- Modify: `Dockerfile`
- Modify: `src/ingestion/config_loader.py`
- Modify: `tests/ingestion/test_config_loader.py`

**Interfaces:**
- Consumes: `config/airports.json`
- Produces: `load_airports_config(config_path: str = "config/airports.json") -> dict[str, dict]` where each airport dict guarantees `"himawari_segment"`, `"outer_half_deg"`, and `"inner_half_deg"`.

- [ ] **Step 1: Write failing test in `tests/ingestion/test_config_loader.py` asserting satellite metadata fields**

```python
def test_load_airports_config_has_satellite_fields():
    cfg = load_airports_config("config/airports.json")
    assert cfg["VVDN"]["himawari_segment"] == "S0410"
    assert cfg["VVNB"]["himawari_segment"] == "S0410"
    assert cfg["VVTS"]["himawari_segment"] == "S0510"
    assert cfg["VVDN"]["outer_half_deg"] == 1.0
    assert cfg["VVDN"]["inner_half_deg"] == 0.25
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_config_loader.py -v`
Expected: FAIL with `KeyError: 'himawari_segment'`

- [ ] **Step 3: Update `config/airports.json`, `src/ingestion/config_loader.py`, `requirements.txt`, and `Dockerfile`**

Add `"himawari_segment"`, `"outer_half_deg": 1.0`, `"inner_half_deg": 0.25` to each airport in `config/airports.json`; add `"himawari_segment"` to `REQUIRED_AIRPORT_FIELDS` in `src/ingestion/config_loader.py` with defaults `outer_half_deg=1.0, inner_half_deg=0.25`; add `satpy>=0.48.0`, `pyresample>=1.28.0`, `numpy>=1.26.0`, `xarray>=2024.1.0`, `matplotlib>=3.8.0`, `dask>=2024.1.0` to `requirements.txt` and install into `.venv`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_config_loader.py -v`
Expected: PASS (3/3 passed)

- [ ] **Step 5: Commit**

```bash
git add config/airports.json src/ingestion/config_loader.py tests/ingestion/test_config_loader.py requirements.txt Dockerfile
git commit -m "feat(module2): add Himawari segment and dual-box config to airports"
```

---

### Task 2: Hadoop HDFS Docker Cluster & WebHDFS Dual-Mode Client

**Files:**
- Modify: `docker-compose.yml`
- Create: `config/hadoop/hadoop.env`
- Create: `src/ingestion/hdfs_client.py`
- Test: `tests/ingestion/test_hdfs_client.py`

**Interfaces:**
- Consumes: `src/ingestion/storage.py`
- Produces:
  - `class WebHDFSClient(namenode_host: str = "localhost", port: int = 9870, user: str = "root")`
  - `WebHDFSClient.is_available(timeout: float = 1.5) -> bool`
  - `WebHDFSClient.upload_file(local_path: str, hdfs_path: str, overwrite: bool = True) -> dict`
  - `WebHDFSClient.sync_directory(local_dir: str = "data", hdfs_root: str = "/data", log_path: str = "data/logs/hdfs_sync_log.jsonl") -> dict`

- [ ] **Step 1: Write failing tests in `tests/ingestion/test_hdfs_client.py`**

```python
import json
from unittest.mock import Mock
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_hdfs_client.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.hdfs_client'`

- [ ] **Step 3: Implement `src/ingestion/hdfs_client.py`, `config/hadoop/hadoop.env`, and Hadoop services in `docker-compose.yml`**

In `src/ingestion/hdfs_client.py`, implement two-step WebHDFS `CREATE` (`PUT /webhdfs/v1/<path>?op=CREATE&overwrite=true&user.name=root` with `allow_redirects=False` followed by `PUT` to the `Location` DataNode URL, or direct follow if `201`), plus `sync_directory` walking `raw/` and `processed/`, measuring `elapsed_ms` and `throughput_mb_s`, and appending a summary record to `hdfs_sync_log.jsonl`. Add `namenode` and `datanode` (`apache/hadoop:3`) to `docker-compose.yml` exposing `9870:9870`, `9000:9000`, `9864:9864`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_hdfs_client.py -v`
Expected: PASS (2/2 passed)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml config/hadoop/hadoop.env src/ingestion/hdfs_client.py tests/ingestion/test_hdfs_client.py
git commit -m "feat(module2): add Hadoop HDFS docker cluster and WebHDFS dual-mode client"
```

---

### Task 3: Himawari-9 S3 Segment Downloader & Retention Manager

**Files:**
- Create: `src/ingestion/himawari_client.py`
- Test: `tests/ingestion/test_himawari_client.py`

**Interfaces:**
- Consumes: `src/ingestion/http_client.py`, `src/ingestion/storage.py`
- Produces:
  - `build_s3_segment_url(scan_dt_utc: datetime, segment: str, band: str = "B13") -> tuple[str, str]`
  - `download_himawari_slot(scan_dt_utc: datetime, segments: list[str] = ["S0410", "S0510"], base_dir: str = "data", is_backfill: bool = False) -> dict`
  - `cleanup_old_raw_segments(base_dir: str = "data", retain_hours: int = 24, pinned_slots: list[str] = None) -> int`

- [ ] **Step 1: Write failing tests in `tests/ingestion/test_himawari_client.py`**

```python
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock
from src.ingestion.himawari_client import (
    build_s3_segment_url,
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_himawari_client.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.himawari_client'`

- [ ] **Step 3: Implement `src/ingestion/himawari_client.py`**

Implement `build_s3_segment_url`, `find_latest_available_slot(now_utc)` (probing `HEAD` on S3 starting from `now_utc - 15 min` rounded down to 10-min slot, stepping back up to 4 slots), `download_himawari_slot` with atomic `.tmp -> .DAT.bz2` write and `append_ingestion_log`, and `cleanup_old_raw_segments`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_himawari_client.py -v`
Expected: PASS (3/3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/himawari_client.py tests/ingestion/test_himawari_client.py
git commit -m "feat(module2): add Himawari-9 B13 S3 segment downloader and retention cleanup"
```

---

### Task 4: Satpy Dual-Box Processor, PNG Renderer & Binary Error Logger

**Files:**
- Create: `src/ingestion/satpy_processor.py`
- Test: `tests/ingestion/test_satpy_processor.py`

**Interfaces:**
- Consumes: `src/ingestion/storage.py`
- Produces:
  - `compute_dual_box_metrics(lons: np.ndarray, lats: np.ndarray, bt_kelvin: np.ndarray, airport_cfg: dict) -> dict`
  - `render_annotated_b13_png(lons: np.ndarray, lats: np.ndarray, bt_kelvin: np.ndarray, airport_cfg: dict, scan_slot_utc: str, output_path: str) -> str`
  - `log_binary_file_error(scan_slot_utc: str, segment: str, file_path: str, error: Exception, is_synthetic_test: bool = False, base_dir: str = "data") -> dict`
  - `process_himawari_slot(scan_slot_utc: str, segment_files: dict[str, str], airports_config: dict[str, dict], base_dir: str = "data", is_backfill: bool = False) -> dict`
  - `inject_synthetic_corrupt_test(base_dir: str = "data") -> dict`

- [ ] **Step 1: Write failing tests in `tests/ingestion/test_satpy_processor.py`**

```python
import bz2
import json
from pathlib import Path
import numpy as np
from src.ingestion.satpy_processor import (
    compute_dual_box_metrics,
    render_annotated_b13_png,
    process_himawari_slot,
    inject_synthetic_corrupt_test,
)

def test_compute_dual_box_metrics_filters_invalid_and_thresholds():
    # Create synthetic 21x21 grid centered at Da Nang (16.0439N, 108.1994E) spanning +-1.0 deg
    lats_1d = np.linspace(15.0439, 17.0439, 21)
    lons_1d = np.linspace(107.1994, 109.1994, 21)
    lons, lats = np.meshgrid(lons_1d, lats_1d)

    bt = np.full_like(lons, 280.0) # Warm background (6.85 C)
    # Center pixel (inside inner 0.25 deg box): severe convection 215.0 K (-58.15 C)
    bt[10, 10] = 215.0
    # Another inner pixel: cold cloud 230.0 K (-43.15 C)
    bt[10, 11] = 230.0
    # Out-of-range noise pixel (<150K) and NaN pixel in outer box
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_satpy_processor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.satpy_processor'`

- [ ] **Step 3: Implement `src/ingestion/satpy_processor.py`**

Implement:
- `validate_hsd_bz2_file(file_path)`: Decompresses and checks non-empty valid HSD header (`b"🕒"` / magic length $\ge 1500$ bytes); raises `ValueError("CorruptBZ2OrHSDError: ...")` on truncated/corrupted bytes.
- `load_segment_grid_with_satpy(file_path, bbox)`: Loads `Scene(filenames=[file_path], reader="ahi_hsd")`, calls `scn.load(["B13"])`, crops to outer bounding box `scn.crop(ll_bbox=(lon_min, lat_min, lon_max, lat_max))`, and extracts `(lons, lats, bt_kelvin)`.
- `compute_dual_box_metrics`: Filters valid range $[150.0, 340.0]\text{ K}$, computes `b13_valid_fraction`, `outer_*` and `inner_*` metrics (`bt_min_k`, `bt_min_c`, `bt_mean_k`, `bt_mean_c`, `cold_cloud_fraction` $<235\text{ K}$, `severe_convection_fraction` $<220\text{ K}$).
- `render_annotated_b13_png`: Renders infrared brightness temperature in Celsius with `matplotlib` (`Agg` backend), marks the airport (`+`), draws the $\pm 0.25^\circ$ inner dashed rectangle, adds colorbar and mandatory JMA/NOAA attribution footer.
- `process_himawari_slot` & `inject_synthetic_corrupt_test`: Saves metrics JSON envelope in `data/raw/himawari_metrics/date=YYYY-MM-DD/` and logs any corrupted file into `data/logs/himawari_file_errors.jsonl`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_satpy_processor.py -v`
Expected: PASS (3/3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/satpy_processor.py tests/ingestion/test_satpy_processor.py
git commit -m "feat(module2): add Satpy dual-box B13 processor, annotated PNG renderer and file error logger"
```

---

### Task 5: CLI Orchestration (`--include-himawari`, `--himawari-window`, `--sync-hdfs`, `--inject-corrupt-test`)

**Files:**
- Modify: `src/ingestion/run_ingest.py`
- Modify: `tests/ingestion/test_run_ingest.py`

**Interfaces:**
- Consumes: `himawari_client`, `satpy_processor`, `hdfs_client`
- Produces: Extended CLI flags on `src.ingestion.run_ingest`:
  - `--include-himawari`: Fetches and processes latest Himawari-9 B13 slot during `--once` or `--loop`
  - `--himawari-window START_ISO END_ISO`: Backfills all 10-minute Himawari-9 slots in `[START_ISO, END_ISO]`
  - `--sync-hdfs`: Syncs local `data/` directory to Hadoop HDFS and records throughput benchmark
  - `--inject-corrupt-test`: Generates labeled synthetic corrupt `.DAT.bz2` file and logs it to `himawari_file_errors.jsonl`
  - `--retain-raw-hours INT`: Retention window for raw `.DAT.bz2` segments (default `24`)

- [ ] **Step 1: Write failing tests in `tests/ingestion/test_run_ingest.py` for the new CLI arguments and window backfill**

```python
def test_parse_args_module2_flags():
    args = parse_args([
        "--once",
        "--include-himawari",
        "--sync-hdfs",
        "--inject-corrupt-test",
        "--himawari-window", "2026-11-09T06:00:00Z", "2026-11-09T08:30:00Z",
        "--retain-raw-hours", "24"
    ])
    assert args.include_himawari is True
    assert args.sync_hdfs is True
    assert args.inject_corrupt_test is True
    assert args.himawari_window == ["2026-11-09T06:00:00Z", "2026-11-09T08:30:00Z"]
    assert args.retain_raw_hours == 24
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\pytest.exe tests/ingestion/test_run_ingest.py -v`
Expected: FAIL with `unrecognized arguments: --include-himawari ...`

- [ ] **Step 3: Update `src/ingestion/run_ingest.py` to integrate Himawari-9, Satpy, and HDFS sync**

Add the CLI flags, implement `run_himawari_window_backfill(start_iso, end_iso, airports_cfg, base_dir)` iterating in 10-minute steps, integrate `--include-himawari` and automatic WebHDFS dual-write/sync into `run_cycle`, and update `run_daemon.bat` & `README.md`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv\Scripts\pytest.exe tests/ -v`
Expected: PASS (all unit tests green)

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/run_ingest.py tests/ingestion/test_run_ingest.py run_daemon.bat README.md
git commit -m "feat(module2): integrate Himawari-9 Satpy pipeline, replay window backfill and HDFS sync into CLI"
```

---

### Task 6: End-to-End Live Verification (Real Himawari-9 Download, Satpy Đà Nẵng/Nội Bài/Tân Sơn Nhất Crop & Corrupt File Test)

**Files:**
- Verify outputs in:
  - `data/raw/himawari_hsd/`
  - `data/raw/himawari_metrics/`
  - `data/processed/himawari_png/airport=VVDN/` (and `VVNB`, `VVTS`)
  - `data/logs/himawari_file_errors.jsonl`
  - `data/logs/ingestion_log.jsonl`

- [ ] **Step 1: Execute real Himawari-9 B13 download & Satpy processing for Replay Window + Synthetic Corrupt Test**

Run:
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --once --include-himawari --inject-corrupt-test
```
And backfill the key replay slots (`07:00Z` single-answer rain at `VVDN` and `08:00Z` co-priority tie at `VVDN` & `VVNB`, plus pre-event sequence):
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --himawari-window 2026-11-09T06:00:00Z 2026-11-09T08:30:00Z
```

- [ ] **Step 2: Verify generated PNG images (starting with Đà Nẵng `VVDN`), metrics JSON values, and `himawari_file_errors.jsonl`**

Check that:
- `data/processed/himawari_png/airport=VVDN/` contains valid annotated PNG images with airport marker and JMA/NOAA attribution.
- `data/raw/himawari_metrics/` contains JSON envelopes with `b13_valid_fraction` $\in (0, 1]$, `inner_bt_min_c`, and `outer_bt_min_c`.
- `data/logs/himawari_file_errors.jsonl` contains the labeled synthetic corrupt test entry.

- [ ] **Step 3: Commit verified artifacts and update project documentation**

```bash
git add data/ docs/
git commit -m "feat(module2): verify live Himawari-9 B13 Satpy processing and replay window artifacts"
```
