# Module 1 (METAR & Open-Meteo Ingestion) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a robust, reproducible data ingestion pipeline that fetches METAR weather observations (every 10m) and Open-Meteo hourly precipitation forecasts (every 60m) for VVDN, VVNB, and VVTS, storing atomic raw JSON envelopes partitioned by UTC date alongside an audit log.

**Architecture:** A modular Python ingestion service packaged for execution via Docker Compose and native `uv` runtime. Atomic writes prevent partial files; resilient HTTP clients with exponential backoff handle network hiccups; all API attempts record audit entries into `ingestion_log.jsonl` to provide empirical proof of 5V Volume/Veracity.

**Tech Stack:** Python 3.11 (`uv`), Docker & Docker Compose, `requests`, `pytest`, `pydantic`/`dataclasses`.

**Spec:** `docs/superpowers/specs/2026-11-09-module1-ingestion-design.md`

## Global Constraints

- Python version: 3.11+
- Partition convention: `data/raw/<source>/date=YYYY-MM-DD/` (UTC)
- Atomic file write: `.tmp` file fsync followed by atomic `os.replace`
- Audit log destination: `data/logs/ingestion_log.jsonl` (JSON Lines format)
- HTTP User-Agent: `VN-Airport-Weather-Dispatcher-Academic-Project/1.0 (MIS5007)`
- Airports monitored: VVDN (Đà Nẵng), VVNB (Nội Bài), VVTS (Tân Sơn Nhất)

## Review Focus

1. Partial file corruption on unexpected crash -> Verified by atomic write test checking absence of `.tmp` and valid JSON in final path.
2. Network timeout or 5xx from Aviation Weather Center -> Verified by mock retry backoff and failure logging with `status="FAILED"`.
3. Malformed JSON payload returned by API -> Verified by storage error boundary recording `status="FAILED"` in `ingestion_log.jsonl`.
4. Empty or missing airports in query -> Verified by configuration validation rejecting invalid ICAO codes.
5. Ingestion replay reproducibility -> Verified by checking metadata envelopes include `fetched_at_utc`, `expected_slot_utc`, and `batch_id`.

---

### Task 1: Scaffolding, Configuration & Environment

**Files:**
- Create: `config/airports.json`
- Create: `requirements.txt`
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `src/__init__.py`
- Create: `src/ingestion/__init__.py`
- Create: `src/ingestion/config_loader.py`
- Test: `tests/ingestion/test_config_loader.py`

**Interfaces:**
- Consumes: Nothing
- Produces: `load_airports_config(config_path: str = "config/airports.json") -> dict[str, dict]`

- [ ] **Step 1: Write the failing test for configuration loader**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `config/airports.json`, `requirements.txt`, `Dockerfile`, `docker-compose.yml`, and `src/ingestion/config_loader.py`**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 2: Resilient Storage & Audit Logger

**Files:**
- Create: `src/ingestion/storage.py`
- Test: `tests/ingestion/test_storage.py`

**Interfaces:**
- Consumes: `config_loader`
- Produces: 
  - `save_raw_envelope(source: str, date_str: str, filename: str, envelope: dict, base_dir: str = "data/raw") -> str`
  - `append_ingestion_log(log_entry: dict, log_path: str = "data/logs/ingestion_log.jsonl") -> None`

- [ ] **Step 1: Write failing tests for atomic write and log appending**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `src/ingestion/storage.py` with atomic write and append-only log**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 3: Resilient HTTP Client with Backoff

**Files:**
- Create: `src/ingestion/http_client.py`
- Test: `tests/ingestion/test_http_client.py`

**Interfaces:**
- Consumes: Standard `requests` library
- Produces: `fetch_with_retry(url: str, params: dict = None, headers: dict = None, max_retries: int = 3, backoff_factor: float = 1.0) -> requests.Response`

- [ ] **Step 1: Write failing test for retry and user-agent header injection**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `fetch_with_retry` in `src/ingestion/http_client.py`**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 4: METAR Ingestion Client

**Files:**
- Create: `src/ingestion/metar_client.py`
- Test: `tests/ingestion/test_metar_client.py`

**Interfaces:**
- Consumes: `http_client.fetch_with_retry`, `storage.save_raw_envelope`, `storage.append_ingestion_log`
- Produces: `ingest_metar(airports: list[str], hours: int = 2, expected_slot_utc: str = None, is_backfill: bool = False, base_dir: str = "data") -> dict`

- [ ] **Step 1: Write failing test with mock Aviation Weather API payload**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `ingest_metar` in `src/ingestion/metar_client.py`**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 5: Open-Meteo Ingestion Client

**Files:**
- Create: `src/ingestion/open_meteo_client.py`
- Test: `tests/ingestion/test_open_meteo_client.py`

**Interfaces:**
- Consumes: `http_client.fetch_with_retry`, `storage.save_raw_envelope`, `storage.append_ingestion_log`
- Produces: `ingest_open_meteo(airports_config: dict, expected_slot_utc: str = None, base_dir: str = "data") -> list[dict]`

- [ ] **Step 1: Write failing test with mock Open-Meteo API payload**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `ingest_open_meteo` in `src/ingestion/open_meteo_client.py`**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 6: CLI Runner & Loop Scheduler

**Files:**
- Create: `src/ingestion/run_ingest.py`
- Test: `tests/ingestion/test_run_ingest.py`

**Interfaces:**
- Consumes: `metar_client`, `open_meteo_client`, `config_loader`
- Produces: `main()` CLI entrypoint accepting `--once`, `--backfill-hours`, and `--loop`

- [ ] **Step 1: Write failing test verifying CLI arguments parser and dispatch logic**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `src/ingestion/run_ingest.py`**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 7: Live Verification & Initial Sample Backfill

**Files:**
- Verify outputs in: `data/raw/metar/`, `data/raw/open_meteo/`, `data/logs/ingestion_log.jsonl`

**Interfaces:**
- Consumes: Full ingestion pipeline
- Produces: Real raw JSON data for 24h backfill across VVDN, VVNB, VVTS and verifiable audit logs

- [ ] **Step 1: Run full unit test suite via `uv run --python 3.11 pytest`**
- [ ] **Step 2: Execute live backfill `--once --backfill-hours 24` to collect real baseline datasets**
- [ ] **Step 3: Inspect raw JSON files and audit log entries to verify schema conformity and byte count**
- [ ] **Step 4: Commit working state and verify readiness for Module 2**

