# Module 4 (Streamlit Decision Dashboard, Historical Replay, KPI Evaluator & Data Quality Audit) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a multi-tab Streamlit decision-support application (`src/dashboard/`) that displays the 3-airport weather review table (`VVDN`, `VVNB`, `VVTS`) with co-priority tie handling and row-order toggling, provides provenance drill-down (raw METAR, point-in-time Open-Meteo forecast, and a 6-frame Himawari-9 B13 image sequence), integrates an interactive KPI trial timer & auto-grader comparing 3-Separate-Sources (Baseline) vs. Dashboard views, and surfaces the PySpark reconciliation, quarantine, and HDFS throughput audit metrics.

**Architecture:** Fast, offline-ready Python data loaders (`src/dashboard/data_loader.py`) read the precomputed PySpark outputs (`data/processed/decision_table/decision_table.jsonl`, `reconciliation_summary.json`, `quarantine_records.jsonl`), annotated PNGs (`data/processed/himawari_png/`), and operational logs (`data/logs/*.jsonl`) in < 1 second, while normalizing timestamps to display both UTC (`HH:MMZ`) and Vietnam time (`HH:MM ICT`, UTC+7). A pure-Python KPI grading engine (`src/dashboard/kpi_evaluator.py`) evaluates dispatcher trial submissions against order-independent ground truth and logs results to `data/logs/kpi_trials_log.jsonl`. The Streamlit UI (`src/dashboard/app.py`) exposes a sidebar for Replay slot selection / optional PySpark re-execution and three main tabs, packaged for both `.venv` Windows (`run_dashboard.bat`) and Docker Compose (`dashboard` service on port `8501`).

**Tech Stack:** Python 3.11, `streamlit>=1.32.0`, `pandas>=2.2.0`, `pytest`.

**Spec:** `docs/superpowers/specs/2026-11-09-module4-streamlit-dashboard-design.md`

## Global Constraints

- Monitored airports: `["VVDN", "VVNB", "VVTS"]` (every review slot displays all 3 airports).
- Timezone presentation: Every review slot and METAR observation timestamp MUST expose both UTC (`HH:MMZ`) and Vietnam time (`HH:MM ICT`, UTC+7), and the KPI auto-grader MUST accept either format when entered by a trial participant.
- Co-priority tie invariance: Reordering the displayed table rows (`priority`, `icao_asc`, `icao_desc`) MUST NEVER change `accepted_first_review_airports` or invalidate any co-priority top airport in KPI grading.
- Satellite attribution: Every displayed Himawari-9 B13 image section MUST display the attribution `Processed Product | Source: JMA/NOAA Himawari-9 B13 via AWS Open Data | Cropped & Annotated by Group`.
- Instant offline response: Reading existing replay slots MUST NOT require an active internet connection or spin up a JVM unless the user explicitly clicks the "Re-run Spark Pipeline" button.

## Review Focus

- Naive local timestamp serialization in `decision_table.jsonl`: PySpark `.collect()` on Windows (`UTC+7`) serializes UTC timestamps (`07:00Z`, `08:00Z`) as local naive ISO strings (`"2026-11-09T14:00:00"`, `"2026-11-09T15:00:00"`), whereas `raw_ob` contains the true UTC token (`090700Z`, `090800Z`). Task 1's `normalize_slot_timestamps` MUST cross-check `raw_ob` (`DDHHMMZ`) so `utc_hhmm` is accurately recovered as `"07:00Z"` / `"08:00Z"` and `vn_hhmm` as `"14:00"` / `"15:00"`.
- Flexible METAR time input during KPI trials: Participants may type `"07:00Z"`, `"0700Z"`, `"090700Z"`, `"07:00"`, `"7:00"`, `"14:00"`, or `"14h00"`. Task 2's `matches_observation_time` MUST normalize and accept all of these equivalents for the selected airport's observation time.
- Order-independent co-priority grading: In the `08:00Z` (`15:00 ICT`) replay slot, both `VVDN` and `VVNB` are Level 1 (`has_precip_or_thunder_at_station = True`). Task 2's test verifies that selecting either `VVDN` or `VVNB` passes even after reversing row order (`sort_mode="icao_desc"`).
- Empty quarantine file on clean runs: When `run_pipeline.py` runs without `--inject-quarantine-test`, `quarantine_records.jsonl` has 0 lines. Task 1's `load_audit_and_reconciliation` MUST gracefully return an empty list for `quarantine_records` while also providing `synthetic_quarantine_preview` from `data/raw/synthetic_quarantine/` so the demo never shows a broken table.
- Future image leakage in 6-frame B13 sequence: `get_b13_image_sequence` for slot `07:30Z` must only return frames `<= 07:30Z` and never include `07:40Z` or `08:00Z` even though those PNGs exist on disk.

---

### Task 1: Dependencies, Docker/Batch Packaging & Dashboard Data Loader (`src/dashboard/data_loader.py`)

**Files:**
- Modify: `requirements.txt`
- Modify: `docker-compose.yml`
- Create: `run_dashboard.bat`
- Create: `src/dashboard/__init__.py`
- Create: `src/dashboard/data_loader.py`
- Create: `tests/dashboard/__init__.py`
- Create: `tests/dashboard/test_data_loader.py`

**Interfaces:**
- Consumes:
  - `data/processed/decision_table/decision_table.jsonl`
  - `data/processed/reconciliation/reconciliation_summary.json`
  - `data/processed/quarantine/quarantine_records.jsonl`
  - `data/processed/himawari_png/airport=<ICAO>/date=<YYYY-MM-DD>/B13_<ICAO>_<YYYYMMDD>_<HHMM>Z.png`
  - `data/logs/himawari_file_errors.jsonl`, `data/logs/ingestion_log.jsonl`, `data/logs/hdfs_sync_log.jsonl`
- Produces:
  - `normalize_timestamp_pair(ts_str: str | None, raw_ob: str | None = None) -> dict` returning `{"utc_iso": str, "utc_hhmm": str, "vn_iso": str, "vn_hhmm": str, "metar_ddhhmmz": str}`
  - `load_decision_slots(storage_root: str = "data") -> dict[str, list[dict]]` keyed by normalized `utc_iso` (`"2026-11-09T07:00:00Z"`, `"2026-11-09T08:00:00Z"`), enriching each row with `slot_utc_hhmm`, `slot_vn_hhmm`, `obs_utc_hhmm`, `obs_vn_hhmm`, and `obs_ddhhmmz`
  - `sort_slot_rows(rows: list[dict], sort_mode: str = "priority") -> list[dict]` (`"priority"`, `"icao_asc"`, `"icao_desc"`)
  - `get_b13_image_sequence(storage_root: str, airport_icao: str, up_to_utc_iso: str, max_frames: int = 6) -> list[dict]` returning `[{"scan_time_utc": str, "utc_hhmm": str, "vn_hhmm": str, "png_path": str}]` ordered chronologically `<= up_to_utc_iso`
  - `load_audit_and_reconciliation(storage_root: str = "data") -> dict` returning reconciliation report, quarantine rows, synthetic quarantine samples, binary file errors, ingestion coverage stats, and HDFS sync stats

- [x] **Step 1: Add `streamlit>=1.32.0` to `requirements.txt`, install in `.venv`, and add `dashboard` service to `docker-compose.yml` + `run_dashboard.bat`**

In `requirements.txt`, append `streamlit>=1.32.0`.
In `docker-compose.yml`, add service `dashboard` building `.`, exposing port `8501:8501`, mounting `./data:/app/data` and `./config:/app/config`, running `streamlit run src/dashboard/app.py --server.port=8501 --server.address=0.0.0.0`.
Create `run_dashboard.bat` launching `.venv\Scripts\streamlit.exe run src/dashboard/app.py`.

- [x] **Step 2: Write failing tests in `tests/dashboard/test_data_loader.py`**

```python
from src.dashboard.data_loader import (
    normalize_timestamp_pair,
    load_decision_slots,
    sort_slot_rows,
    get_b13_image_sequence,
    load_audit_and_reconciliation,
)


def test_normalize_timestamp_pair_recovers_utc_from_raw_ob():
    info = normalize_timestamp_pair(
        "2026-11-09T14:00:00",
        raw_ob="METAR VVDN 090700Z 09008KT 8000 -RA",
    )
    assert info["utc_iso"] == "2026-11-09T07:00:00Z"
    assert info["utc_hhmm"] == "07:00Z"
    assert info["vn_hhmm"] == "14:00"
    assert info["metar_ddhhmmz"] == "090700Z"


def test_load_decision_slots_and_sort_modes_preserve_co_priority():
    slots = load_decision_slots("data")
    assert "2026-11-09T07:00:00Z" in slots
    assert "2026-11-09T08:00:00Z" in slots

    slot_08 = slots["2026-11-09T08:00:00Z"]
    assert len(slot_08) == 3

    asc = sort_slot_rows(slot_08, "icao_asc")
    desc = sort_slot_rows(slot_08, "icao_desc")
    assert [r["airport_icao"] for r in asc] == ["VVDN", "VVNB", "VVTS"]
    assert [r["airport_icao"] for r in desc] == ["VVTS", "VVNB", "VVDN"]
    for r in desc:
        assert r["accepted_first_review_airports"] == ["VVDN", "VVNB"]


def test_get_b13_image_sequence_no_future_leakage():
    seq = get_b13_image_sequence("data", "VVDN", "2026-11-09T07:30:00Z", max_frames=6)
    assert 1 <= len(seq) <= 6
    assert seq[-1]["utc_hhmm"] == "07:30Z"
    for item in seq:
        assert item["utc_hhmm"] <= "07:30Z"


def test_load_audit_and_reconciliation_returns_all_sections():
    audit = load_audit_and_reconciliation("data")
    assert audit["reconciliation"]["all_sources_reconciled"] is True
    assert len(audit["binary_file_errors"]) >= 1
    assert audit["ingestion_summary"]["total_logs"] >= 1
    assert len(audit["synthetic_quarantine_preview"]) >= 1
```

- [x] **Step 3: Run test to verify it fails**

Run: `.venv\Scripts\pytest.exe tests/dashboard/test_data_loader.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.dashboard.data_loader'`

- [x] **Step 4: Implement `src/dashboard/data_loader.py`**

Implement `normalize_timestamp_pair`, `load_decision_slots`, `sort_slot_rows`, `get_b13_image_sequence`, and `load_audit_and_reconciliation`.
In `normalize_timestamp_pair(ts_str, raw_ob)`:
- Extract regex `\b(\d{2})(\d{2})(\d{2})Z\b` from `raw_ob` if provided; use its `HH:MM` as the canonical UTC hour/minute on the date of `ts_str`, and compute Vietnam time as `UTC + 7 hours`.
- If `raw_ob` is not provided and `ts_str` has no `"Z"` or `"+00:00"`, check if `ts_str` hour is `>= 13` while matching a `2026-11-09` replay slot (`14:00` $\rightarrow$ `07:00Z`, `15:00` $\rightarrow$ `08:00Z`), or treat ISO strings ending in `Z` directly as UTC.

- [x] **Step 5: Run test to verify it passes**

Run: `.venv\Scripts\pytest.exe tests/dashboard/test_data_loader.py -v`
Expected: PASS (4 passed)

- [x] **Step 6: Commit**

```bash
git add requirements.txt docker-compose.yml run_dashboard.bat src/dashboard/__init__.py src/dashboard/data_loader.py tests/dashboard/__init__.py tests/dashboard/test_data_loader.py
git commit -m "feat(dashboard): add replay data loader, UTC/VN time normalizer and B13 sequence finder"
```

---

### Task 2: KPI Trial Auto-Grader, Log Persistence & Median Calculator (`src/dashboard/kpi_evaluator.py`)

**Files:**
- Create: `src/dashboard/kpi_evaluator.py`
- Create: `tests/dashboard/test_kpi_evaluator.py`
- Create: `data/logs/kpi_trials_log.jsonl` (seeded with realistic baseline vs. dashboard trial records marked `is_sample_seed: true` so the KPI comparison table works out-of-the-box)

**Interfaces:**
- Consumes: `slot_rows: list[dict]` produced by `load_decision_slots` in Task 1
- Produces:
  - `REASON_OPTIONS: dict[str, str]` mapping reason codes (`"PRECIP_OR_THUNDER_AT_STATION"`, `"LOW_VISIBILITY"`, `"STALE_OR_MISSING_METAR"`, `"FORECAST_RAIN_NEXT_HOUR"`, `"ROUTINE"`) to Vietnamese labels
  - `derive_expected_reason_code(airport_row: dict) -> str`
  - `matches_observation_time(entered_str: str, airport_row: dict) -> bool`
  - `evaluate_kpi_submission(slot_rows: list[dict], selected_airport: str, selected_reason_code: str, entered_obs_time: str, elapsed_seconds: float, status: str = "COMPLETED") -> dict`
  - `append_kpi_trial(trial_record: dict, log_path: str = "data/logs/kpi_trials_log.jsonl") -> None`
  - `load_kpi_trials(log_path: str = "data/logs/kpi_trials_log.jsonl", include_seeds: bool = True) -> list[dict]`
  - `compute_kpi_summary(trials: list[dict]) -> dict`

- [ ] **Step 1: Write failing tests in `tests/dashboard/test_kpi_evaluator.py`**

```python
from src.dashboard.data_loader import load_decision_slots, sort_slot_rows
from src.dashboard.kpi_evaluator import (
    evaluate_kpi_submission,
    matches_observation_time,
    compute_kpi_summary,
    append_kpi_trial,
    load_kpi_trials,
)


def test_evaluate_single_answer_slot_0700():
    slots = load_decision_slots("data")
    row_07 = slots["2026-11-09T07:00:00Z"]

    # Correct airport (VVDN), correct reason, UTC time
    res_utc = evaluate_kpi_submission(
        row_07,
        selected_airport="VVDN",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="07:00Z",
        elapsed_seconds=38.5,
    )
    assert res_utc["is_passed"] is True

    # Correct airport (VVDN), correct reason, Vietnam time (14:00)
    res_vn = evaluate_kpi_submission(
        row_07,
        selected_airport="VVDN",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="14:00",
        elapsed_seconds=35.0,
    )
    assert res_vn["is_passed"] is True

    # Wrong airport (VVTS)
    res_wrong = evaluate_kpi_submission(
        row_07,
        selected_airport="VVTS",
        selected_reason_code="ROUTINE",
        entered_obs_time="07:00Z",
        elapsed_seconds=40.0,
    )
    assert res_wrong["is_passed"] is False
    assert "WRONG_AIRPORT" in res_wrong["failure_codes"]


def test_evaluate_co_priority_slot_0800_order_independent():
    slots = load_decision_slots("data")
    row_08_reversed = sort_slot_rows(slots["2026-11-09T08:00:00Z"], "icao_desc")

    # Both VVDN and VVNB must be accepted even when table order is reversed
    res_vvdn = evaluate_kpi_submission(
        row_08_reversed,
        selected_airport="VVDN",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="090800Z",
        elapsed_seconds=42.0,
    )
    res_vvnb = evaluate_kpi_submission(
        row_08_reversed,
        selected_airport="VVNB",
        selected_reason_code="PRECIP_OR_THUNDER_AT_STATION",
        entered_obs_time="15:00",
        elapsed_seconds=39.0,
    )
    assert res_vvdn["is_passed"] is True
    assert res_vvnb["is_passed"] is True


def test_compute_kpi_summary_calculates_median_and_reduction(tmp_path):
    log_file = tmp_path / "kpi_trials.jsonl"
    sample_trials = [
        {"view_mode": "baseline_3_sources", "is_passed": True, "elapsed_seconds": 95.0, "status": "COMPLETED"},
        {"view_mode": "baseline_3_sources", "is_passed": True, "elapsed_seconds": 105.0, "status": "COMPLETED"},
        {"view_mode": "baseline_3_sources", "is_passed": False, "elapsed_seconds": 120.0, "status": "COMPLETED"},
        {"view_mode": "dashboard", "is_passed": True, "elapsed_seconds": 40.0, "status": "COMPLETED"},
        {"view_mode": "dashboard", "is_passed": True, "elapsed_seconds": 50.0, "status": "COMPLETED"},
    ]
    for t in sample_trials:
        append_kpi_trial(t, str(log_file))

    loaded = load_kpi_trials(str(log_file))
    summary = compute_kpi_summary(loaded)
    assert summary["baseline_3_sources"]["median_passed_seconds"] == 100.0
    assert summary["baseline_3_sources"]["passed_trials"] == 2
    assert summary["baseline_3_sources"]["total_trials"] == 3
    assert summary["dashboard"]["median_passed_seconds"] == 45.0
    assert summary["time_reduction_pct"] == 55.0
    assert summary["meets_30_pct_target"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\pytest.exe tests/dashboard/test_kpi_evaluator.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.dashboard.kpi_evaluator'`

- [ ] **Step 3: Implement `src/dashboard/kpi_evaluator.py` and seed `data/logs/kpi_trials_log.jsonl`**

Implement `evaluate_kpi_submission`, `matches_observation_time`, `append_kpi_trial`, `load_kpi_trials`, and `compute_kpi_summary`. Seed `data/logs/kpi_trials_log.jsonl` with 8 initial trial records (4 `baseline_3_sources` and 4 `dashboard` across `07:00Z` and `08:00Z`, all tagged `"is_sample_seed": true`) so the KPI dashboard tab displays a complete comparison table immediately upon startup.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv\Scripts\pytest.exe tests/dashboard/test_kpi_evaluator.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/dashboard/kpi_evaluator.py tests/dashboard/test_kpi_evaluator.py data/logs/kpi_trials_log.jsonl
git commit -m "feat(dashboard): add KPI trial auto-grader, UTC/VN time matcher and median evaluator"
```

---

### Task 3: Multi-Tab Streamlit Dashboard Application (`src/dashboard/app.py`) & Smoke Tests

**Files:**
- Create: `src/dashboard/app.py`
- Create: `tests/dashboard/test_app_smoke.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: `src/dashboard/data_loader.py` and `src/dashboard/kpi_evaluator.py`
- Produces:
  - Streamlit application in `src/dashboard/app.py` with:
    - **Sidebar:**
      - Chọn mốc rà soát phát lại (`review_slot_utc`, hiển thị cả giờ UTC `07:00Z`/`08:00Z` và giờ VN `14:00 ICT`/`15:00 ICT`)
      - Chọn thứ tự sắp xếp bảng (`Mức ưu tiên (Mặc định)`, `Mã sân bay A → Z`, `Đảo thứ tự dòng Z → A (Kiểm thử đồng mức)`)
      - Khung tùy chọn **"Chạy lại Pipeline PySpark"** (có checkbox `Bơm dữ liệu lỗi kiểm thử (--inject-quarantine-test)`)
    - **Tab 1 — 🎯 Bảng Quyết định & Truy nguyên Bằng chứng:**
      - Banner kết luận lượt rà soát: Hiển thị sân bay cần kiểm tra đầu tiên (`accepted_first_review_airports`), tự động nổi bật cảnh báo **ĐỒNG MỨC ƯU TIÊN ĐẦU (Co-Priority Tie)** ở ca `08:00Z` (`15:00 ICT`).
      - Bảng tổng hợp 3 dòng (`VVDN`, `VVNB`, `VVTS`) với đầy đủ: Mức ưu tiên, Lý do tiếng Việt, Giờ quan trắc METAR (UTC & VN), Tuổi dữ liệu, Tầm nhìn (m + cờ giới hạn 9999), Dự báo mưa giờ tới, Phân loại mây B13 (`b13_summary_vi`, `inner_bt_min_c`), và Trạng thái bằng chứng 3 nguồn.
      - Khu vực **Truy nguyên Bằng chứng Chi tiết (Drill-Down Inspector)** cho sân bay được chọn:
        - Cột 1: Bản tin METAR gốc (`raw_ob`), mã hiện tượng tại trạm/lân cận/xu hướng (`TEMPO`), phiên bản sửa, và thông tin dự báo Open-Meteo.
        - Cột 2: Ảnh vệ tinh Himawari-9 B13 gần nhất kèm **Thanh trượt / Lưới Chuỗi 6 Ảnh Trước Sự kiện (Time-Lapse 10 phút/ảnh)**, bảng chỉ số Dual-Box ($\pm 1.0^\circ$ và $\pm 0.25^\circ$) và dòng ghi công bản quyền bắt buộc.
    - **Tab 2 — ⏱️ Đo lường & Chấm điểm Tự động KPI (CLO4):**
      - Chế độ chuyển đổi trực tiếp giữa **Xem 3 Nguồn Rời (Baseline)** và **Xem Bảng Tổng hợp (Dashboard)** để phục vụ đo thực nghiệm Tuần 3.
      - Đồng hồ bấm giờ nhận ca (`Bắt đầu nhận ca` / `Đặt lại đồng hồ`) và Biểu mẫu nộp kết quả rà soát (Mã người thử, Cách xem, Thứ tự thử, Sân bay chọn đầu tiên, Lý do, Giờ METAR làm căn cứ).
      - Kết quả chấm tự động tức thì (`ĐẠT` / `CHƯA ĐẠT` kèm giải thích đáp án chuẩn) và Bảng Thống kê KPI Tổng hợp (Số lượt đạt/tổng lượt, Trung vị thời gian lượt đạt của 2 cách xem, % rút ngắn thời gian so với mục tiêu 30%).
    - **Tab 3 — 🛡️ Đối soát Dữ liệu, Quarantine & Ma trận Lưu trữ (CLO2/CLO3):**
      - Bảng đối soát bảo toàn dòng (`rows_read == rows_valid_pre_dedup + rows_quarantined`) từ `reconciliation_summary.json`.
      - Trình xem bản ghi lỗi `quarantine` (kèm bản xem trước `synthetic_quarantine` và nhật ký tệp nhị phân hỏng `himawari_file_errors.jsonl`).
      - Thống kê thực nghiệm 5V (`ingestion_log.jsonl`) và tốc độ đồng bộ Hadoop WebHDFS (`hdfs_sync_log.jsonl`).

- [ ] **Step 1: Write failing smoke test in `tests/dashboard/test_app_smoke.py`**

```python
from streamlit.testing.v1 import AppTest


def test_streamlit_app_renders_without_exception():
    at = AppTest.from_file("src/dashboard/app.py", default_timeout=30)
    at.run()
    assert not at.exception
    assert len(at.tabs) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\pytest.exe tests/dashboard/test_app_smoke.py -v`
Expected: FAIL (file `src/dashboard/app.py` does not exist)

- [ ] **Step 3: Implement `src/dashboard/app.py` and update `README.md`**

Implement `src/dashboard/app.py` with clean Vietnamese labels, clear visual hierarchy, and full error resilience. Update `README.md` with instructions for launching the Streamlit dashboard via `run_dashboard.bat`, `.venv\Scripts\streamlit.exe run src/dashboard/app.py`, or `docker compose up -d dashboard`.

- [ ] **Step 4: Run all dashboard tests and full repository test suite**

Run:
`.venv\Scripts\pytest.exe tests/dashboard/ -v`
`.venv\Scripts\pytest.exe -k "not spark" -v`
Expected: All dashboard and ingestion/crawler tests PASS with 0 errors.

- [ ] **Step 5: Commit**

```bash
git add src/dashboard/app.py tests/dashboard/test_app_smoke.py README.md docs/superpowers/plans/2026-11-09-module4-streamlit-dashboard.md
git commit -m "feat(dashboard): implement multi-tab Streamlit decision dashboard, replay inspector and KPI timer"
```

