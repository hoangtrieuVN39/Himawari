# Dashboard Review Fixes & Compact Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sửa đủ 6 lỗi đã xác nhận trong review `main` so với merge base `cbc7628941151b80333df9c39098abf885f180e6`, đồng thời giảm số file dữ liệu API bằng JSONL theo nguồn/ngày và migration có kiểm chứng.

**Architecture:** Phần A (Task 1–6) sửa dashboard/grader theo cấu trúc hiện có. Phần B (Task 7–11) thêm `RawEnvelopeStore` dùng chung, gộp envelope vào JSONL theo nguồn/ngày, chuyển reader Spark/dashboard và bổ sung migration/backup. Kiểm thử trên dữ liệu tạm; kho đang hoạt động chỉ chuyển layout sau khi đối chiếu dữ liệu và kết quả pipeline.

**Tech Stack:** Python 3.11, pytest, Streamlit testing, PySpark 3.5.x, pathlib, unittest.mock; Phần B bổ sung `portalocker>=3.2,<5` cho khóa file liên tiến trình.

**Spec:** `docs/superpowers/specs/2026-11-09-module4-streamlit-dashboard-design.md` và `docs/superpowers/specs/2026-11-10-compact-raw-storage-design.md`. Sáu finding review ngày 2026-11-10 ánh xạ vào Task 1–6; yêu cầu bổ sung giảm số file ánh xạ vào Task 7–11. Đọc cả hai spec trước khi triển khai.

## Global Constraints

- Ba sân bay: `VVDN`, `VVNB`, `VVTS`; mỗi mốc có đúng 3 dòng.
- UTC: `HH:MMZ`; giờ Việt Nam: `HH:MM ICT`, UTC+7.
- `forecast_fetched_at_utc <= review_slot_utc` đối với dữ liệu dự báo LIVE.
- METAR mới: tuổi `<= 60.0` phút; quá cũ: `> 60.0` phút; tầm nhìn thấp: `< 5000` m; cảnh báo mưa dự báo: `>= 0.1` mm/h.
- Giữ `accepted_first_review_airports` độc lập thứ tự hiển thị.
- KPI dùng median của lượt đạt; mục tiêu giảm thời gian `30%`.
- Replay hoạt động offline; đo tốc độ đọc khi nguồn không đổi với mục tiêu `< 1 giây` trên bộ dữ liệu hiện có.
- Hỗ trợ `.venv` Windows và Docker; giữ `streamlit>=1.32.0`. Phần A không thêm dependency; Phần B chỉ thêm thư viện khóa file đã nêu.
- Repo đang có thay đổi và dữ liệu chưa track trong `data/`. Không reset, stage, sửa fixture trực tiếp tại đó hoặc đưa chúng vào commit sửa lỗi.
- Giữ công thức KPI và logic nghiệp vụ Spark/live; thay phần lưu/đọc vật lý qua module chung.
- Phần B: JSONL UTF-8, một envelope/dòng; gộp 3 sân bay cùng nguồn/ngày, shard `64 * 1024 * 1024` byte; metadata và batch_id được bảo toàn.
- Migration phải verify trước cutover; chỉ một layout active. Prune chỉ thực hiện trên file nằm trong báo cáo đã verify và đã có backup checksum đúng.

## Review Focus

1. CLI tùy chọn và đường dẫn có dấu/khoảng trắng: truyền nguyên một argument, parser ingestion chấp nhận (Task 1).
2. Một hoặc cả ba sân bay thiếu METAR: render đủ bảng và drill-down, tuổi dữ liệu không biến thành 0 (Task 2).
3. Forecast đến sau lần đọc đầu, kể cả khi đã có mốc LIVE mới hơn: mốc cũ được cập nhật khi forecast đủ điều kiện (Task 3).
4. METAR mưa/tầm nhìn thấp ở biên 60 phút và thời gian có suffix UTC/ICT: grader đồng nhất với ưu tiên và múi giờ (Task 4–5).
5. Nộp trước Start, sau Reset hoặc khi đồng hồ hệ thống thay đổi: không tạo thời gian giả và không đưa lượt chưa đo vào median (Task 6).

Review Focus bổ sung cho lưu trữ: hai writer đồng thời/retry ID (Task 7), bytes ghi và đường dẫn provenance (Task 8), file ngày đổi nhưng tên giữ nguyên và JSONL có dòng lỗi (Task 9), migration bị ngắt hoặc chạy lại (Task 10), HDFS lỗi từng phần hoặc dataset không đổi (Task 11).

## File Map

| File | Trách nhiệm sau sửa |
| --- | --- |
| `src/dashboard/data_loader.py` | Gọi CLI đúng; phát hiện nguồn thay đổi và làm mới mốc LIVE |
| `src/dashboard/app.py` | Render tuổi METAR có thể thiếu; kiểm soát vòng đời đo thời gian |
| `src/dashboard/kpi_evaluator.py` | Chọn lý do theo độ mới; so khớp giờ theo suffix |
| `tests/dashboard/conftest.py` (mới) | Dữ liệu mẫu sao chép vào `tmp_path`, fixtures dùng chung |
| `tests/dashboard/test_live_sync.py` (mới) | CLI contract và nguồn đến muộn |
| `tests/dashboard/test_app_regressions.py` (mới) | AppTest cho thiếu METAR và timer |
| `tests/dashboard/test_data_loader.py` | Dùng bản sao dữ liệu thay cho `data/` thật |
| `tests/dashboard/test_kpi_evaluator.py` | Bổ sung ca stale/timezone; dùng dữ liệu và log tạm |
| `tests/dashboard/test_app_smoke.py` | Mock loader/persistence để smoke test không ghi dữ liệu thật |
| `src/ingestion/raw_store.py` (mới) | Chọn layout, ghi JSONL nguyên tử, khóa, đọc envelope và signature |
| `src/ingestion/storage.py` | Giữ writer JSON legacy cho tương thích và synthetic fixtures |
| `src/ingestion/metar_client.py`, `open_meteo_client.py`, `satpy_processor.py` | Ghi qua store và dùng write receipt |
| `src/spark/readers.py`, `src/spark/run_pipeline.py` | Đọc layout được chọn, dùng multiline đúng định dạng |
| `src/ingestion/migrate_storage.py` (mới) | Dry-run, migrate, verify, activate, backup/prune và rollback |
| `src/ingestion/hdfs_client.py` | Snapshot nguồn active, bỏ qua file không đổi và backup |
| `tests/ingestion/test_raw_store.py`, `test_migrate_storage.py` (mới) | Concurrency, idempotency, file count, migration và recovery |
| `tests/spark/test_storage_layout_parity.py` (mới) | So sánh reader/pipeline giữa legacy và JSONL |
| `requirements.txt`, `.gitignore`, `README.md` | Dependency khóa file, runtime artifacts và hướng dẫn vận hành |

## Task 1: Khôi phục nút cập nhật LIVE — P1

**Files:** `src/dashboard/data_loader.py:396`, `tests/dashboard/test_live_sync.py`, `tests/dashboard/conftest.py`; điều chỉnh fixtures của ba file test hiện có trong File Map.

**Interfaces:** Giữ `fetch_and_update_realtime(storage_root: str = "data", include_himawari: bool = True) -> dict[str, Any]`. Fixture `isolated_data(tmp_path) -> Path` sao chép các JSON/JSONL, PNG cần thiết từ bộ mẫu; mọi loader test dùng đường dẫn này. UI test mock các hàm nạp và ghi được import trong `app`.

- [ ] Tạo fixtures cô lập và chuyển test hiện có sang bản sao dữ liệu; chạy 13 test hiện tại, kỳ vọng PASS và `git status --short` không xuất hiện thay đổi dữ liệu mới.
- [ ] Viết `test_realtime_refresh_uses_ingestor_cli_contract`, parameterize `include_himawari=True/False`. Mock `subprocess.run`; kiểm tra đối số qua `src.ingestion.run_ingest.parse_args(cmd[3:])`, `args.base_dir == str(isolated_data)` và cờ Himawari đúng. Dùng thư mục con có dấu/khoảng trắng. Mock sync/load để không chạy mạng hoặc Spark.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/dashboard/test_live_sync.py -k cli_contract -q`; kỳ vọng FAIL do parser không nhận `--storage-root`.
- [ ] Đổi flag thành `--base-dir`, giữ argv dạng list và `sys.executable`; giữ cấu trúc kết quả `SUCCESS`/`ERROR`.
- [ ] Thêm ca returncode khác 0: kết quả `status == "ERROR"`, không gọi sync; chạy lại test file, kỳ vọng PASS.
- [ ] Tạo commit riêng cho CLI và test isolation sau khi xác nhận diff không chứa dữ liệu người dùng.

## Task 2: Render được trường hợp thiếu METAR — P1

**Files:** `src/dashboard/app.py:249,282`, `tests/dashboard/test_app_regressions.py`.

**Interfaces:** Thêm helper nội bộ `_format_metar_age(age_minutes: float | None) -> str`, dùng chung ở bảng và drill-down. `None -> "N/A"`; giá trị hợp lệ hiển thị số phút. Không thay đổi giá trị dữ liệu gốc.

- [ ] Viết `test_missing_metar_renders_without_exception` bằng `AppTest.from_string`: mock loader trả đúng 3 dòng, lần lượt một sân bay hoặc cả ba có `metar_is_present=False`, `metar_age_minutes=None`, ưu tiên 2. Mock ảnh, audit và KPI log; chọn sân bay thiếu dữ liệu trong drill-down. Assert `not at.exception`, bảng còn đủ 3 dòng và tuổi hiển thị `N/A`.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/dashboard/test_app_regressions.py -k missing_metar -q`; kỳ vọng FAIL với `int(None)` hoặc format `None`.
- [ ] Thêm helper và thay cả hai phép format tuổi hiện tại. Thêm assertion `0.0` hiển thị `0 phút`, không bị coi là thiếu.
- [ ] Chạy lại ca thiếu METAR và smoke test; kỳ vọng PASS, cả ba tab render được.
- [ ] Tạo commit riêng cho null-safe rendering.

## Task 3: Cập nhật lại mốc LIVE khi nguồn đến muộn — P2

**Files:** `src/dashboard/data_loader.py:331`, `tests/dashboard/test_live_sync.py`.

**Interfaces:** Giữ `sync_latest_raw_to_decision_table(storage_root: str = "data", force_latest: bool = False) -> list[str]` và `load_decision_slots(...)`. Thêm `_raw_input_signature(storage_root: str) -> tuple[tuple[str, int, int], ...]`, gồm đường dẫn tương đối, size và `mtime_ns` đã sort của JSON METAR/Open-Meteo/Himawari metrics. Cache nội bộ theo `Path(storage_root).resolve()`, chứa signature nguồn và signature decision table sau lần sync thành công. Khi tới Task 9, helper này gọi `RawEnvelopeStore.signature()` để bao gồm layout marker và JSONL, giữ nguyên hợp đồng cache.

**Lựa chọn triển khai:** Khi signature thay đổi hoặc chưa có cache, tính lại tất cả mốc phát hiện từ METAR không-backfill, kể cả mốc đã tồn tại. Khi nguồn và bảng quyết định không đổi, bỏ qua tính lại; `force_latest=True` vẫn ép tính mốc cuối. Giữ nguyên các mốc replay ngoài tập LIVE phát hiện được. Đây là giải pháp ưu tiên tính đúng cho quy mô repo hiện tại; không thêm database hoặc file cache bền vững.

- [ ] Viết `test_existing_live_slot_refreshes_after_late_forecast`: METAR fetched `08:01Z`, obs `08:00Z`; lần sync đầu tạo slot `08:05Z`, VVDN Level 4. Sau đó thêm forecast fetched `08:02Z`, valid `09:00Z`, rain `5.0`; lần load tiếp phải cho rain `5.0`, Level 3, không thêm dòng trùng.
- [ ] Thêm `test_older_live_slot_refreshes_after_late_input`: tạo thêm slot `08:15Z` trước khi forecast cho ca trên xuất hiện; slot `08:05Z` vẫn phải được cập nhật. Thêm ca forecast fetched `08:06Z` không được vào slot `08:05Z`.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/dashboard/test_live_sync.py -k 'late or older' -q`; kỳ vọng FAIL vì slot cũ bị skip.
- [ ] Implement signature/cache và invalidation như trên. Ghi bảng qua file tạm cùng thư mục rồi `Path.replace`; chỉ cache sau ghi thành công. Nếu nguồn đổi giữa đầu/cuối lần tính, không đánh dấu cache hợp lệ để lần load sau tính lại.
- [ ] Thêm kiểm chứng: nguồn không đổi không gọi assembler lần nữa; sửa JSON cùng tên vẫn làm mới; thay bảng bằng kết quả Spark làm mất hiệu lực cache; hai storage root độc lập; replay `07:00Z`/`08:00Z` và tập đồng ưu tiên được giữ nguyên. `force_latest` vẫn chạy khi cache hợp lệ.
- [ ] Chạy toàn bộ `test_live_sync.py` và `test_data_loader.py`, kỳ vọng PASS. Đo nhiều lần load sau warm-up trên bản sao bộ mẫu, ghi median và kiểm tra mục tiêu `< 1 giây`; không dùng ngưỡng wall-clock dễ nhiễu làm unit assertion.
- [ ] Tạo commit riêng cho invalidation nguồn. Ghi nhận chi phí lần recompute khi nguồn thay đổi trong mô tả commit/PR.

## Task 4: Grader dùng cùng điều kiện độ mới với priority engine — P2

**Files:** `src/dashboard/kpi_evaluator.py:24,181`, `tests/dashboard/test_kpi_evaluator.py`.

**Interfaces:** Giữ `derive_expected_reason_code(airport_row: dict[str, Any]) -> str` và `evaluate_kpi_submission(...) -> dict[str, Any]`.

- [ ] Viết `test_stale_hazard_requires_data_verification_reason`, parameterize mưa, tầm nhìn thấp và cả hai: METAR tuổi `90.0`, ưu tiên 2; reason `STALE_OR_MISSING_METAR` phải PASS, reason mưa/tầm nhìn phải có `WRONG_REASON`. Giờ quan trắc và sân bay đều hợp lệ trong fixture.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/dashboard/test_kpi_evaluator.py -k stale_hazard -q`; kỳ vọng FAIL.
- [ ] Kiểm tra thiếu/quá cũ trước các cờ thời tiết. Chỉ chấp nhận hai lý do mưa/tầm nhìn thay thế nhau khi METAR đủ mới và cả hai điều kiện cùng đúng; không để nhánh alternative reason bỏ qua freshness.
- [ ] Thêm biên `60.0` phút còn mới, `60.1` phút đã cũ; kiểm tra missing-METAR reason bằng `derive_expected_reason_code`. Giữ hành vi helper với dict tối thiểu của các test hiện tại.
- [ ] Chạy toàn bộ `test_kpi_evaluator.py`, bao gồm đồng ưu tiên đảo dòng; kỳ vọng PASS.
- [ ] Tạo commit riêng cho freshness grading.

## Task 5: Tôn trọng múi giờ khi chấm giờ quan trắc — P2

**Files:** `src/dashboard/kpi_evaluator.py:42`, `tests/dashboard/test_kpi_evaluator.py`.

**Interfaces:** Giữ `matches_observation_time(entered_str: str, airport_row: dict[str, Any]) -> bool`.

**Quy tắc:** `Z`/`UTC` chỉ so với UTC; `ICT`/`VN` chỉ so với UTC+7. Chuỗi giờ không suffix vẫn chấp nhận một trong hai theo spec. Chuỗi 6 số `DDHHMM[Z]` là mã METAR UTC và phải khớp cả ngày, giờ, phút.

- [ ] Viết `test_time_match_respects_explicit_timezone`: obs `07:00Z`/`14:00 ICT`, `090700Z`; assert TRUE cho `07:00Z`, `0700Z`, `07:00 UTC`, `14:00 ICT`, `14h00 VN`, `07:00`, `14:00`; assert FALSE cho `14:00Z`, `1400Z`, `07:00ICT`, `07h00 VN`, `100700Z`.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/dashboard/test_kpi_evaluator.py -k explicit_timezone -q`; kỳ vọng FAIL.
- [ ] Parse suffix thành thông tin rõ ràng trước khi so khớp; dùng full-match, kiểm tra giờ `0..23`, phút `0..59`, và ngày trong mã METAR. Không dùng direct-match shortcut bỏ qua suffix.
- [ ] Thêm ca UTC `23:30` -> ICT `06:30` ngày kế tiếp; ca nhập `24:00`, `07:60` phải FALSE. Giữ các định dạng cũ `7:00`, `14h`, `14` và mã `090700` hợp lệ.
- [ ] Chạy toàn bộ test grader, kỳ vọng PASS.
- [ ] Tạo commit riêng cho timezone matching.

## Task 6: Chỉ ghi KPI với thời gian đo thật — P2

**Files:** `src/dashboard/app.py:49,428,435,449,478`, `tests/dashboard/test_app_regressions.py`.

**Interfaces:** Giữ schema log KPI và API evaluator. Thêm session key `timer_has_started: bool`, mặc định False. Start đặt True; Reset đặt False. Dùng `time.perf_counter()` cho khoảng thời gian; chỉ thời điểm ghi log dùng UTC wall clock như hiện tại.

**Hành vi chọn cho bản sửa:** Cho phép nộp khi đồng hồ đang chạy hoặc đã Stop, miễn lượt đó đã Start. Chưa Start hoặc đã Reset thì hiển thị hướng dẫn bắt đầu và không gọi grader/persistence. Xóa hoàn toàn fallback `45.0`; không suy diễn hoặc sửa các log 45 giây cũ vì không thể phân biệt số đo thật với số giả.

- [ ] Viết `test_submit_without_start_does_not_record_trial`: đáp án đúng, bấm Submit khi idle; assert không gọi `append_kpi_trial` và không xuất hiện PASS. Thêm ca Start -> Reset -> Submit có cùng kết quả.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/dashboard/test_app_regressions.py -k 'without_start or reset' -q`; kỳ vọng FAIL vì hiện tại ghi 45 giây.
- [ ] Implement guard dựa trên trạng thái đã Start, dùng đồng hồ monotonic cho Start/Stop/Submit và bỏ giá trị mặc định giả. Phân biệt thời gian đo được bằng 0 sau làm tròn với trạng thái chưa Start.
- [ ] Viết AppTest với clock cố định: Start tại `100.0`, Submit tại `112.5` ghi đúng `12.5`; Start -> Stop tại `112.5` -> Submit tại `150.0` vẫn ghi `12.5`. Giả lập wall clock nhảy lùi không làm duration âm. Mock persistence, assert số lần ghi và `is_sample_seed=False`.
- [ ] Chạy toàn bộ `test_app_regressions.py` và smoke test, kỳ vọng PASS; summary của tập lượt đo cố định vẫn đúng median.
- [ ] Tạo commit riêng cho measured timing.

## Kiểm chứng Phần A

- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/dashboard/ -q`; toàn bộ test cũ và mới PASS, không gọi mạng và không sửa `data/` thật.
- [ ] Kiểm tra diff cho cả 6 task; chạy `git diff --check`. Đối chiếu `git status --short` với trạng thái trước triển khai để xác nhận giữ nguyên dữ liệu đang sửa dở.
- [ ] Smoke test bằng AppTest: replay một đáp án, replay đồng ưu tiên đảo dòng, LIVE cập nhật thành công với subprocess giả, thiếu METAR, KPI thiếu Start và KPI đã đo.
- [ ] Chỉ chạy thêm ingestion/Spark tests nếu implementation thực tế thay đổi module của chúng hoặc có dấu hiệu lỗi xuyên module; không cần bật Docker/API cho regression của 6 finding này.
- [ ] Báo cáo từng finding đã sửa, số test PASS và kết quả đo replay. Không tuyên bố đã kiểm chứng tải API thật hoặc Docker nếu chỉ dùng mock/offline.

## Phần B: Giảm số file dữ liệu API

**Đề xuất:** Mỗi nguồn/ngày dùng `batches-0001.jsonl`, tự chia shard khi vượt 64 MiB. Ở lịch daemon hiện tại và kích thước payload hiện có, 360 envelope/ngày khi bật vệ tinh sẽ nằm trong khoảng 3 file dữ liệu/ngày thay vì 360 JSON. PNG/HSD dùng cơ chế riêng. Lựa chọn, tradeoff, bố cục file và rollback được mô tả đầy đủ trong spec lưu trữ mới.

## Task 7: Module lưu trữ JSONL theo nguồn/ngày

**Files:** Create `src/ingestion/raw_store.py`, `tests/ingestion/test_raw_store.py`; Modify `requirements.txt`, `.gitignore`.

**Interfaces:** Triển khai đúng `WriteReceipt`, `ReadSpec`, `RawEnvelopeStore.__init__`, `write`, `iter_envelopes`, `read_specs`, `signature` trong spec lưu trữ. Đường dẫn kho mới là `raw_compact`, layout marker là `storage_state.json`.

- [ ] Viết `test_daily_store_keeps_all_batches_in_three_files`: ghi 144 METAR, 72 forecast của 3 sân bay và 144 metrics trong cùng ngày; assert 360 batch đọc lại đúng payload, đúng 3 JSONL khi dưới giới hạn shard. Thêm ca ngày UTC kế tiếp có file riêng và shard vượt 64 MiB (test điều chỉnh giới hạn nội bộ để fixture nhỏ).
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/ingestion/test_raw_store.py -q`; kỳ vọng FAIL vì store chưa có.
- [ ] Implement lựa chọn layout, serialization một dòng/envelope, receipt và exclusive lock timeout 10 giây. Ghi shard bằng temp + flush/fsync + replace dưới root lock; không append trực tiếp vào file published. Giữ API JSON legacy hiện có cho compatibility.
- [ ] Thêm test hai process ghi đồng thời bằng multiprocessing, không mất batch; retry cùng ID/nội dung trả `inserted=False`, `bytes_added=0`; ID trùng khác nội dung báo conflict; batch khác cùng payload vẫn giữ cả hai. Dùng helper process importable trên Windows.
- [ ] Thêm test crash trước replace giữ file cũ; reader không thấy dòng bị cắt; marker hỏng báo lỗi; kho cũ không marker chọn legacy, kho rỗng chọn JSONL; lock timeout không báo thành công. `iter_envelopes(strict=False)` phải log vị trí envelope lỗi.
- [ ] Thêm ignore cho runtime compact data, marker, lock, staging và backup; không bỏ track fixture đã có. Chạy test store và test_storage legacy, kỳ vọng PASS; commit module riêng.

## Task 8: Chuyển các producer sang store và sửa số đo log

**Files:** Modify `src/ingestion/metar_client.py`, `src/ingestion/open_meteo_client.py`, `src/ingestion/satpy_processor.py`; tests tương ứng trong `tests/ingestion/`.

**Interfaces:** Các hàm ingest giữ chữ ký và shape kết quả đang dùng. Mỗi batch gọi `RawEnvelopeStore(base_dir).write(source, date_str, envelope)`; log lấy `file_path`, `batch_id`, `bytes_written` từ receipt, thêm `storage_layout`.

- [ ] Viết `test_ingestion_batches_share_daily_file` cho METAR/forecast/metrics bằng HTTP/Satpy mocks: hai lần gọi có ID khác dùng chung file nguồn/ngày nhưng đọc lại đủ hai envelope.
- [ ] Chạy các test mới; kỳ vọng FAIL vì producer vẫn tạo JSON riêng.
- [ ] Thay các call site `save_raw_envelope` và `os.path.getsize(saved_path)` tương ứng. `bytes_written=receipt.bytes_added`; không tính tổng file ngày thành byte của mỗi lần API. Không thay metadata thời tiết, forecast version hay nhãn backfill.
- [ ] Thêm test batch thứ hai có bytes_written bằng chiều dài UTF-8 envelope mới + newline, không bằng tổng hai batch; kiểm tra saved_path Himawari và provenance batch_id vẫn truy nguyên được.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/ingestion/test_metar_client.py tests/ingestion/test_open_meteo_client.py tests/ingestion/test_satpy_processor.py tests/ingestion/test_run_ingest.py -q`, kỳ vọng PASS; commit producer migration.

## Task 9: Reader Spark/dashboard dùng chung layout

**Files:** Modify `src/spark/readers.py`, `src/spark/run_pipeline.py`, `src/dashboard/data_loader.py`; Create `tests/spark/test_storage_layout_parity.py`; extend dashboard loader/live-sync tests.

**Interfaces:** Giữ các hàm `read_raw_metar`, `read_raw_open_meteo`, `read_raw_himawari_metrics`; thêm keyword-only `multiline: bool = True` để caller cũ/synthetic vẫn dùng JSON. Pipeline lấy `ReadSpec` từ store. Dashboard dùng `iter_envelopes` và `signature`; discovery latest slot lấy timestamp metadata lớn nhất, không dựa filename.

- [ ] Tạo hai kho tạm có envelope tương đương, một legacy và một daily_jsonl; viết `test_storage_layouts_produce_same_pipeline_results`. Assert bằng nhau các key/giá trị nghiệp vụ, số dòng read/quarantine/dedup/retained, priority và accepted airports tại các slot cố định; bỏ qua physical input path và timestamp ghi audit.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/spark/test_storage_layout_parity.py -q`; kỳ vọng FAIL khi JSONL chưa được đọc.
- [ ] Thay hardcoded glob `*.json` tại pipeline, latest discovery và các vòng đọc raw của dashboard bằng store. Dùng schema cũ, `multiLine=False` cho JSONL. Không thay transforms, priority hoặc policy point-in-time.
- [ ] Thêm test JSONL có 3 dòng, dòng giữa malformed: Spark đọc đủ hai dòng tốt và ghi nhận dòng lỗi qua quarantine; synthetic `.json` vẫn đọc như trước. Python reader không bỏ im lặng lỗi định dạng.
- [ ] Chạy lại các test Task 3 với cả hai layout; append envelope vào cùng file ngày phải làm mới forecast/slot cũ. Thay marker từ legacy sang compact phải invalidate cache; mỗi lần load chỉ dùng một layout, không đếm đôi.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/spark/ tests/dashboard/ -q`, kỳ vọng PASS; ghi benchmark replay không đổi nguồn cho cả hai layout; commit reader changes.

## Task 10: Migration, backup và giảm file vật lý

**Files:** Create `src/ingestion/migrate_storage.py`, `tests/ingestion/test_migrate_storage.py`; Modify `README.md`.

**Interfaces:** CLI `python -m src.ingestion.migrate_storage --base-dir <root> --dry-run|--apply|--prune-legacy|--rollback`. Public `migrate_storage(storage_root: str, *, apply: bool = False) -> dict` trả report: status, source/target counts, checksum mismatches, corrupt files, conflicts, active layout và report path. Prune/rollback dùng report đã verify, không dùng glob xóa tự do.

- [ ] Viết `test_migration_dry_run_is_read_only`: chụp file tree/checksum trước sau, hoàn toàn không đổi; báo đúng 74 -> số file gộp theo source/ngày trên bản sao dữ liệu hiện có. Số cụ thể file đích tính từ fixture, không hardcode chỉ có một ngày.
- [ ] Viết `test_migration_preserves_batches_and_replay`: giữ nguyên toàn bộ metadata/payload, bao gồm batch khác ID có payload giống, envelope thiếu batch_id và nhiều occurrence cùng ID/cùng nội dung. So sánh multiset canonical checksum, không dùng dict theo batch_id làm mất occurrence. File JSON malformed hoặc cùng ID/nội dung khác phải chặn activation; mọi nguồn cũ vẫn nguyên vẹn.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/ingestion/test_migrate_storage.py -q`; kỳ vọng FAIL vì CLI/module chưa có.
- [ ] Implement staging + report ánh xạ từng legacy_relative_path sang checksum/shard/thứ tự dòng, root lock, checksum chuẩn hóa theo occurrence, pipeline parity bằng cùng review slots; đổi marker nguyên tử cuối cùng khi tất cả check PASS. Migration dùng snapshot writer nội bộ để giữ occurrence, không dùng `write` có dedup batch mới. Writer đọc lại marker sau lấy lock. Apply yêu cầu cửa sổ tạm dừng ingestion; không gọi API trong migration.
- [ ] Thêm crash test trước/sau publish và trước marker; legacy vẫn usable trước cutover. Chạy lại apply sau activation là no-op, không ghi đè batch mới. Kiểm tra báo cáo rollback trước/sau khi có batch mới; rollback sau cutover phải giữ cả dữ liệu mới như spec.
- [ ] Implement `--prune-legacy`: ZIP backup exact bytes + relative paths, verify checksum, chỉ xóa file trong danh sách đã migrate và không đổi kể từ lúc migrate. Resolve từng path, xác nhận nằm trong đúng source directory dưới storage_root; giữ nguyên HSD, PNG, synthetic và file ngoài report. Test backup lỗi hoặc source đổi thì không xóa.
- [ ] Chạy test migration và parity trên bản sao repo; assert giảm số file vật lý sau prune, không mất batch/replay. Ghi dry-run/apply/prune/rollback và thời điểm dừng daemon vào README; commit migration tooling. Chưa tự chạy prune trên dữ liệu thật trong bước viết code.

## Task 11: HDFS và nghiệm thu toàn luồng

**Files:** Modify `src/ingestion/hdfs_client.py`, `tests/ingestion/test_hdfs_client.py`, `README.md`; thêm test storage end-to-end trong `tests/spark/test_storage_layout_parity.py`.

**Interfaces:** Giữ `WebHDFSClient.sync_directory(...) -> dict`. Thêm trường kết quả `files_skipped_unchanged`; dùng một manifest checksum tại `data/logs/hdfs_sync_manifest.json`, key gồm HDFS destination và relative path, chỉ ghi checksum sau upload thành công. Chọn file raw qua store; processed/HSD theo chính sách hiện có.

- [ ] Viết `test_sync_only_active_storage_layout`: kho có legacy, compact, ZIP backup, lock và staging nhưng chỉ layout active cùng processed/HSD được chọn. `test_unchanged_second_sync_skips_upload` phải có 0 upload dữ liệu ở lần hai; sửa một JSONL chỉ upload file đổi và metadata cần thiết.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/ingestion/test_hdfs_client.py -q`; kỳ vọng FAIL trước thay đổi.
- [ ] Implement snapshot file dưới root lock rồi upload ngoài lock; không khóa ingestion trong thời gian mạng. Với kho mới, manifest ghi một lần vào cuối sync, không tự đưa manifest vào danh sách upload.
- [ ] Khi cutover HDFS, chỉ publish marker remote mới sau khi tập file cần thiết upload đầy đủ. Lỗi từng phần không đổi marker và không đánh dấu file lỗi đã đồng bộ; lần sau retry. Spark trên HDFS được chạy sau sync thành công, không đồng thời với thao tác thay snapshot; mô tả rõ giới hạn này trong README.
- [ ] Test upload thất bại, nguồn đổi giữa sync và lần sau, marker chưa chuyển khi thiếu file, cùng relative path nhưng khác HDFS destination không dùng nhầm cache. Network tests dùng mock; ghi rõ nếu chưa kiểm thử Docker/HDFS thật.
- [ ] Chạy `.venv\Scripts\python.exe -m pytest tests/ingestion/ tests/spark/ tests/dashboard/ -q`; toàn bộ PASS. Tất cả dữ liệu ghi ở tmp_path; chạy `git diff --check` và so trạng thái `data/` với trước triển khai.
- [ ] Đo file count trước/sau cho 360 fixture batch/ngày, thời gian write đầu/cuối ngày, thời gian replay và bytes HDFS lần sync thứ hai; báo cáo số thực đo thay vì suy đoán.
- [ ] Commit integration/docs, review lại 6 finding ban đầu và mọi điểm migration/concurrency; kết quả bàn giao gồm mã sửa, regression tests, report migration trên bản sao và lệnh vận hành cụ thể.

## Thứ tự và cách triển khai đề xuất

Phần A: Task 1 -> 2 -> 3 -> 4 -> 5 -> 6. Phần B: Task 7 -> 8 -> 9 -> 10 -> 11, triển khai sau khi Phần A qua kiểm chứng. Task 9 thay helper signature của Task 3 bằng store; giữ nguyên regression về nguồn đến muộn.

Hai phần có mốc nghiệm thu riêng để có thể đưa bản sửa dashboard vào dùng trước khi chuyển kho. Đề xuất triển khai tuần tự, kiểm tra sau mỗi task. Hiện chỉ cập nhật kế hoạch và thiết kế; chưa sửa product code, chưa migrate hoặc prune dữ liệu thật.
