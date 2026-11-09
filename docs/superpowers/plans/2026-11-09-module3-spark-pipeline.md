# Module 3 (PySpark ETL Pipeline, Quarantine, Temporal Join & Priority Rules) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible, Dockerized PySpark ETL pipeline that reads raw METAR, Open-Meteo forecast, and Himawari-9 B13 Dual-Box metrics with strict schemas, separates invalid/out-of-range records into a traceable `quarantine` table with invariant row-count reconciliation, normalizes METAR visibility and weather phenomena, performs point-in-time joins without future data leakage, applies Level 1–4 review priority rules (including co-priority ties), and outputs Parquet/JSONL decision tables for the three Vietnam airports (`VVDN`, `VVNB`, `VVTS`).

**Architecture:** PySpark (`local[*]` with UTC session timezone inside the `spark-processor` Docker container) ingests raw JSON envelopes from local storage (`/app/data/raw`) or Hadoop HDFS (`hdfs://namenode:9000/data/raw`). Five modular stages—Schema Readers (`readers.py`), Quarantine & Deduplication/Reconciliation (`quarantine.py`), Meteorological Normalization (`transforms.py`), Point-in-Time Temporal Join (`temporal_join.py`), and Priority Ranking Engine (`priority_engine.py`)—produce partitioned Parquet and JSONL datasets under `data/processed/`.

**Tech Stack:** Python 3.11, OpenJDK 17 (`openjdk-17-jre-headless` in Docker), Apache Spark / `pyspark>=3.5.0`, `pyarrow>=15.0.0`, `pandas>=2.2.0`, `pytest`.

**Spec:** `docs/superpowers/specs/2026-11-09-module3-spark-pipeline-design.md`

## Global Constraints

- Execution environment: All PySpark commands and `pytest tests/spark/` MUST run inside the Docker container (`docker compose run --rm spark-processor ...`).
- Monitored airports: `["VVDN", "VVNB", "VVTS"]` (every review slot MUST output exactly 3 rows, one per airport, even when sources are missing).
- Spark session timezone: `spark.sql.session.timeZone = "UTC"`.
- Physical Brightness Temperature valid bounds: $[150.0\text{ K}, 340.0\text{ K}]$ ($[-123.15^\circ\text{C}, +66.85^\circ\text{C}]$).
- Cloud fraction valid bounds: $[0.0, 1.0]$.
- Low visibility threshold for Level 1 priority: `visibility_m < 5000` (5 km).
- Stale METAR threshold for Level 2 verification: `metar_age_minutes > 60.0` (60 minutes) or missing METAR.
- Forecast rain threshold for Level 3 alert: `forecast_rain_next_hour_mm >= 0.1` mm/h.
- Reconciliation invariant: `rows_read == rows_valid_pre_dedup + rows_quarantined` and `rows_valid_pre_dedup == rows_retained_valid + rows_duplicate_dropped`.

## Review Focus

1. METAR trend forecast (`TEMPO 3000 TSRA` or `BECMG ... RA`) vs. current station weather -> Must NOT trigger Level 1 hazard at station when the main body before `TEMPO`/`BECMG` reports fair weather (`9999 FEW020`).
2. METAR vicinity weather (`VCTS`, `VCSH`, `VCFG`) -> Must be flagged as `has_vicinity_weather = True` and excluded from `has_precip_or_thunder_at_station = True`.
3. Future forecast or late-arriving data leakage (`fetched_at_utc > review_slot_utc`) -> Must be strictly excluded from the point-in-time snapshot at `review_slot_utc` in live/strict mode.
4. Corrected/special METAR reports (`COR` or `SPECI` for the same `observed_at_utc` arriving at a later `fetched_at_utc`) -> Must retain both revisions in `valid_metar` and select the revision whose `fetched_at_utc <= review_slot_utc` is latest.
5. Co-priority ties at the top level (e.g., both `VVDN` and `VVNB` at Level 1) -> Both rows must receive `priority_level = 1`, `is_top_priority = True`, `is_co_priority_top = True`, and `accepted_first_review_airports = ["VVDN", "VVNB"]` regardless of input row order.

---

### Task 1: Docker PySpark Environment & Shared SparkSession Fixture

**Files:**
- Modify: `Dockerfile`
- Modify: `requirements.txt`
- Modify: `docker-compose.yml`
- Create: `src/spark/__init__.py`
- Create: `src/spark/session.py`
- Create: `tests/spark/__init__.py`
- Create: `tests/spark/conftest.py`
- Test: `tests/spark/test_session.py`

**Interfaces:**
- Consumes: `Dockerfile`, `docker-compose.yml`, `requirements.txt`
- Produces:
  - `get_spark_session(app_name: str = "AirportWeatherReviewSpark", master: str = "local[*]") -> SparkSession` in `src/spark/session.py`
  - `spark` pytest session fixture in `tests/spark/conftest.py`
  - Docker Compose service `spark-processor` capable of running `pytest tests/spark/ -v` and `python -m src.spark.run_pipeline`

- [ ] **Step 1: Update `Dockerfile`, `requirements.txt`, and `docker-compose.yml` with OpenJDK 17 and PySpark**

Add `openjdk-17-jre-headless` (and `procps`) via `apt-get` in `Dockerfile`, set `JAVA_HOME`, add `pyspark>=3.5.0`, `pyarrow>=15.0.0`, `pandas>=2.2.0` to `requirements.txt`, and add service `spark-processor` in `docker-compose.yml` mounting `./data:/app/data`, `./config:/app/config`, `./src:/app/src`, and `./tests:/app/tests`.

- [ ] **Step 2: Write the failing test in `tests/spark/test_session.py`**

```python
from src.spark.session import get_spark_session


def test_spark_session_utc_and_basic_dataframe():
    spark = get_spark_session("TestSession")
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    df = spark.createDataFrame([("VVDN", 1), ("VVNB", 2)], ["airport_icao", "rank"])
    assert df.count() == 2
```

- [ ] **Step 3: Implement `get_spark_session` in `src/spark/session.py` and fixture in `tests/spark/conftest.py`**

Configure `spark.sql.session.timeZone = "UTC"`, `spark.sql.execution.arrow.pyspark.enabled = "true"`, `spark.ui.enabled = "false"`, and `spark.sql.shuffle.partitions = "4"` for fast local/Docker execution.

- [ ] **Step 4: Build `spark-processor` image and run test inside Docker**

Run: `docker compose build spark-processor && docker compose run --rm spark-processor pytest tests/spark/test_session.py -v`
Expected: PASS (`1 passed`)

- [ ] **Step 5: Commit**

```bash
git add Dockerfile requirements.txt docker-compose.yml src/spark/__init__.py src/spark/session.py tests/spark/__init__.py tests/spark/conftest.py tests/spark/test_session.py
git commit -m "feat(spark): add Docker OpenJDK/PySpark runtime and UTC SparkSession factory"
```

---

### Task 2: Strict PySpark Schemas & Raw Envelope Readers

**Files:**
- Create: `src/spark/schemas.py`
- Create: `src/spark/readers.py`
- Test: `tests/spark/test_schemas_and_readers.py`

**Interfaces:**
- Consumes: Raw JSON envelopes in `data/raw/{metar,open_meteo,himawari_metrics}/`
- Produces:
  - `METAR_ENVELOPE_SCHEMA`, `OPEN_METEO_ENVELOPE_SCHEMA`, `HIMAWARI_METRICS_ENVELOPE_SCHEMA` in `src/spark/schemas.py`
  - `read_raw_metar(spark: SparkSession, path: str) -> DataFrame`
  - `read_raw_open_meteo(spark: SparkSession, path: str) -> DataFrame`
  - `read_raw_himawari_metrics(spark: SparkSession, path: str) -> DataFrame`

- [ ] **Step 1: Write failing tests in `tests/spark/test_schemas_and_readers.py`**

Test reading valid sample envelopes for all 3 sources as well as malformed JSON files (capturing `_corrupt_record` without failing the job) and attaching `batch_id`, `fetched_at_utc`, `is_backfill`, `is_synthetic_test`, and `input_file_path`.

- [ ] **Step 2: Run test in Docker to verify it fails**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_schemas_and_readers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.spark.schemas'`

- [ ] **Step 3: Implement `src/spark/schemas.py` and `src/spark/readers.py`**

Use `spark.read.schema(...).option("multiLine", True).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json(path)`. Explode `payload` for METAR, `arrays_zip` + `explode` for Open-Meteo `hourly` arrays, and `explode` `airports` for Himawari B13 metrics.

- [ ] **Step 4: Run test in Docker to verify it passes**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_schemas_and_readers.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/spark/schemas.py src/spark/readers.py tests/spark/test_schemas_and_readers.py
git commit -m "feat(spark): add strict schemas and raw JSON envelope readers for all 3 sources"
```

---

### Task 3: Quarantine Splitter, Deduplication, Versioning & Reconciliation Engine

**Files:**
- Create: `src/spark/quarantine.py`
- Test: `tests/spark/test_quarantine_and_reconcile.py`

**Interfaces:**
- Consumes: DataFrames from `read_raw_metar`, `read_raw_open_meteo`, `read_raw_himawari_metrics`, plus `data/logs/himawari_file_errors.jsonl`
- Produces:
  - `validate_and_split_metar(df: DataFrame, valid_airports: list[str]) -> tuple[DataFrame, DataFrame, dict]`
  - `validate_and_split_open_meteo(df: DataFrame, valid_airports: list[str]) -> tuple[DataFrame, DataFrame, dict]`
  - `validate_and_split_himawari(df: DataFrame, valid_airports: list[str]) -> tuple[DataFrame, DataFrame, dict]`
  - `build_reconciliation_report(metrics_by_source: list[dict], file_error_log_path: str) -> dict`
  - `build_data_quality_profile(valid_metar_df: DataFrame, valid_forecast_df: DataFrame, valid_himawari_df: DataFrame, quarantine_df: DataFrame) -> dict`
  - `inject_synthetic_quarantine_batch(raw_root: str) -> dict`

- [ ] **Step 1: Write failing tests in `tests/spark/test_quarantine_and_reconcile.py`**

Include tests for:
1. Quarantine error codes: `SCHEMA_VIOLATION`, `UNKNOWN_AIRPORT` (`"VVXX"`), `INVALID_TIMESTAMP`, `FUTURE_OBSERVATION_TIME`, `OUT_OF_RANGE_BT_KELVIN` ($120.0\text{ K}$ and $410.0\text{ K}$), `OUT_OF_RANGE_CLOUD_FRACTION` ($1.25$), and `NEGATIVE_FORECAST_VALUE` ($-2.0\text{ mm}$).
2. Deduplication vs. METAR revision preservation: identical `(airport_icao, observed_at_utc, raw_ob)` drops duplicates and increments `rows_duplicate_dropped`, whereas a corrected report (`COR` / `SPECI`) with the same `(airport_icao, observed_at_utc)` but different `raw_ob` is retained with `revision_index = 2`.
3. Invariant reconciliation equations: `rows_read == rows_valid_pre_dedup + rows_quarantined` and `rows_valid_pre_dedup == rows_retained_valid + rows_duplicate_dropped`.

- [ ] **Step 2: Run test in Docker to verify it fails**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_quarantine_and_reconcile.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `src/spark/quarantine.py`**

Implement array-based `error_codes` evaluation using PySpark SQL functions (`array_remove`, `when`, `lit`), split into `valid_pre_dedup_df` (`size(error_codes) == 0`) and `quarantine_df` (`size(error_codes) > 0`), apply Window deduplication & revision numbering, and compute reconciliation & data quality profile dictionaries.

- [ ] **Step 4: Run test in Docker to verify it passes**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_quarantine_and_reconcile.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/spark/quarantine.py tests/spark/test_quarantine_and_reconcile.py
git commit -m "feat(spark): add quarantine validation, revision deduplication and reconciliation engine"
```

---

### Task 4: Meteorological Normalization (Visibility, Phenomenon Parser & B13 Cloud Classification)

**Files:**
- Create: `src/spark/transforms.py`
- Test: `tests/spark/test_transforms.py`

**Interfaces:**
- Consumes: Validated `metar` and `himawari_metrics` DataFrames
- Produces:
  - `parse_metar_raw_ob(raw_ob: str | None, visib_raw: str | None) -> dict`
  - `enrich_valid_metar(df: DataFrame) -> DataFrame` (adds `visibility_m`, `visibility_capped_9999`, `is_low_visibility`, `has_precip_or_thunder_at_station`, `station_weather_codes`, `has_vicinity_weather`, `vicinity_weather_codes`, `trend_weather_codes`, `is_amended_or_speci`)
  - `enrich_valid_himawari(df: DataFrame) -> DataFrame` (adds `b13_cloud_tag`, `b13_summary_vi`)

- [ ] **Step 1: Write failing tests in `tests/spark/test_transforms.py`**

Test exact cases:
1. `"METAR VVDN 090700Z 08007KT 9999 -RA SCT020 BKN070 27/24 Q1016 NOSIG"` -> `visibility_m == 10000`, `visibility_capped_9999 is True`, `is_low_visibility is False`, `has_precip_or_thunder_at_station is True`, `station_weather_codes == ["-RA"]`.
2. `"METAR VVNB 090800Z 05008KT 2000 RA BR BKN012 24/23 Q1015"` -> `visibility_m == 2000`, `visibility_capped_9999 is False`, `is_low_visibility is True`, `has_precip_or_thunder_at_station is True`, `station_weather_codes == ["RA"]`.
3. `"METAR VVTS 090830Z 05006KT 9999 VCTS FEW025CB SCT060 33/24 Q1009 TEMPO 3000 TSRA"` -> `has_precip_or_thunder_at_station is False`, `has_vicinity_weather is True`, `vicinity_weather_codes == ["VCTS"]`, `trend_weather_codes == ["TSRA"]`.
4. `"SPECI VVDN 091505Z COR 11015G25KT 4000 +TSRA BKN015CB 25/24 Q1012"` -> `is_amended_or_speci is True`, `visibility_m == 4000`, `is_low_visibility is True`, `has_precip_or_thunder_at_station is True`, `station_weather_codes == ["+TSRA"]`.
5. Himawari B13 classification: `inner_severe_convection_fraction = 0.12` -> `"SEVERE_CONVECTION_AT_STATION"`; `inner_cold_cloud_fraction = 0.08` -> `"COLD_CONVECTIVE_CLOUD_AT_STATION"`.

- [ ] **Step 2: Run test in Docker to verify it fails**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_transforms.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `src/spark/transforms.py`**

Implement deterministic regex parser splitting `raw_ob` at `\b(TEMPO|BECMG|NOSIG|RMK)\b`, extracting metric 4-digit visibility / `CAVOK` / statute-mile fallback, separating station precipitation/thunderstorm tokens from `VC*` vicinity and `TEMPO`/`BECMG` trend tokens, and wrapping in a typed PySpark UDF / native expressions.

- [ ] **Step 4: Run test in Docker to verify it passes**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_transforms.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/spark/transforms.py tests/spark/test_transforms.py
git commit -m "feat(spark): add METAR visibility/phenomenon normalizer and B13 cloud classifier"
```

---

### Task 5: Point-in-Time Temporal Join & Priority Ranking Engine (Level 1–4 + Co-Priority Ties)

**Files:**
- Create: `src/spark/temporal_join.py`
- Create: `src/spark/priority_engine.py`
- Test: `tests/spark/test_temporal_join.py`
- Test: `tests/spark/test_priority_engine.py`

**Interfaces:**
- Consumes: Enriched `valid_metar`, `valid_open_meteo`, and `valid_himawari` DataFrames
- Produces:
  - `build_point_in_time_snapshot(spark: SparkSession, review_slots_utc: list[str], metar_df: DataFrame, forecast_df: DataFrame, himawari_df: DataFrame, airports: list[str], mode: str = "live") -> DataFrame` in `src/spark/temporal_join.py`
  - `apply_priority_rules(snapshot_df: DataFrame) -> DataFrame` in `src/spark/priority_engine.py`

- [ ] **Step 1: Write failing tests in `tests/spark/test_temporal_join.py` and `tests/spark/test_priority_engine.py`**

Cover:
1. **No Future Leakage:** Forecast fetched at `15:15Z` is excluded when `review_slot_utc = "2026-11-09T15:00:00Z"`, and included when `review_slot_utc = "2026-11-09T15:20:00Z"`.
2. **Amended METAR (`COR`/`SPECI`) Progression:** Original `15:00Z` observation (received `15:02Z`, fair weather) is selected at `review_slot_utc = 15:05Z`, while the `COR` revision (received `15:08Z`, `TSRA`) is selected at `review_slot_utc = 15:10Z`.
3. **Pre-Event B13 Image Sequence:** Collects up to 6 PNG paths in `[review_slot_utc - 50m, review_slot_utc]` ordered chronologically.
4. **Locked Scenario 1 (Single Top Answer at 15:00Z):** `VVDN` has fresh METAR with `TSRA` (Level 1); `VVNB` has 90-minute old METAR (Level 2); `VVTS` has fresh fair METAR + next-hour forecast rain $1.8\text{ mm}$ (Level 3) -> `accepted_first_review_airports == ["VVDN"]`, `is_co_priority_top == False`.
5. **Locked Scenario 2 (Co-Priority Top Tie & Order Invariance):** `VVDN` has fresh METAR with `TS` and visibility $6000\text{ m}$ (Level 1); `VVNB` has fresh METAR with `RA` and visibility $2000\text{ m}$ (Level 1); `VVTS` has normal conditions (Level 4) -> Both `VVDN` and `VVNB` have `priority_level == 1`, `is_top_priority == True`, `is_co_priority_top == True`, `accepted_first_review_airports == ["VVDN", "VVNB"]`. Reversing the input DataFrame row order yields identical priority fields and `accepted_first_review_airports`.

- [ ] **Step 2: Run tests in Docker to verify they fail**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_temporal_join.py tests/spark/test_priority_engine.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `src/spark/temporal_join.py` and `src/spark/priority_engine.py`**

Implement the 3-airport spine cross-join with `review_slots_utc`, point-in-time window filtering (`fetched_at_utc <= review_slot_utc` in `live` mode, backfill-aware in `replay` mode), 6-image sequence aggregation, Level 1–4 `when` expression chain, Vietnamese evidence summary generation, and Window-based `is_top_priority` / `is_co_priority_top` / `accepted_first_review_airports` calculation.

- [ ] **Step 4: Run tests in Docker to verify they pass**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_temporal_join.py tests/spark/test_priority_engine.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/spark/temporal_join.py src/spark/priority_engine.py tests/spark/test_temporal_join.py tests/spark/test_priority_engine.py
git commit -m "feat(spark): add point-in-time temporal join and Level 1-4 priority engine"
```

---

### Task 6: End-to-End Pipeline CLI, Parquet/JSONL Writers, Real Replay Verification & Documentation

**Files:**
- Create: `src/spark/run_pipeline.py`
- Modify: `README.md`
- Test: `tests/spark/test_run_pipeline.py`

**Interfaces:**
- Consumes: All modules in `src/spark/` and `config/airports.json`
- Produces:
  - CLI `python -m src.spark.run_pipeline` supporting `--storage-root` (`data` or `hdfs://namenode:9000/data`), `--review-slots` (comma-separated ISO UTC timestamps or `latest`), `--mode` (`live` or `replay`), and `--inject-quarantine-test`
  - Output datasets under `data/processed/`: `valid_metar/`, `valid_open_meteo/`, `valid_himawari_metrics/`, `quarantine/`, `data_profile/`, `reconciliation/`, `decision_table/`

- [ ] **Step 1: Write failing integration & idempotency tests in `tests/spark/test_run_pipeline.py`**

Verify that:
1. Running `run_spark_pipeline` twice on the same input produces identical key counts (`3 * len(review_slots)`), identical priority decisions, and identical quarantine row counts.
2. Missing one source completely for an airport still outputs all 3 airport rows for every review slot with explicit missing-source flags.

- [ ] **Step 2: Run test in Docker to verify it fails**

Run: `docker compose run --rm spark-processor pytest tests/spark/test_run_pipeline.py -v`
Expected: FAIL

- [ ] **Step 3: Implement `src/spark/run_pipeline.py` and update `README.md`**

Implement the full pipeline orchestrator writing deterministic Parquet + JSONL outputs, run against our real collected dataset for replay slots `2026-11-09T07:00:00Z` (single-airport rain at `VVDN`) and `2026-11-09T08:00:00Z` (co-priority rain at `VVDN` + `VVNB`) plus `--inject-quarantine-test`, and document Docker usage in `README.md`.

- [ ] **Step 4: Run full test suite (Module 1 + Module 2 + Module 3) and execute real pipeline in Docker**

Run:
`docker compose run --rm spark-processor pytest tests/ -v`
`docker compose run --rm spark-processor python -m src.spark.run_pipeline --mode replay --review-slots 2026-11-09T07:00:00Z,2026-11-09T08:00:00Z --inject-quarantine-test`
Expected: All tests PASS and `data/processed/decision_table/`, `data/processed/quarantine/`, and `data/processed/reconciliation/` are generated and verified.

- [ ] **Step 5: Commit**

```bash
git add src/spark/run_pipeline.py tests/spark/test_run_pipeline.py README.md data/processed/
git commit -m "feat(spark): add end-to-end PySpark pipeline CLI, reconciliation outputs and replay verification"
```
