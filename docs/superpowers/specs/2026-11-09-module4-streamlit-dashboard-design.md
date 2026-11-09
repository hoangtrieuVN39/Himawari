# Tài liệu Thiết kế (Spec) — Mô-đun 4: Dashboard Streamlit Rà soát Quyết định Thời tiết, Phát lại Lịch sử (Replay), Đo lường KPI & Đối soát Chất lượng Dữ liệu

**Ngày tạo:** 2026-11-09  
**Dự án:** Hỗ trợ điều phối viên rà soát thời tiết tại ba sân bay Việt Nam (`VVDN`, `VVNB`, `VVTS`)  
**Phạm vi:** Mô-đun 4 (Mốc Tuần 2 · Buổi 5–6 & Tuần 3 · Buổi 7–8 — Xây dựng ứng dụng Dashboard bằng Streamlit đọc tức thì từ các bảng kết quả đã xử lý của PySpark, hiển thị bảng quyết định 3 sân bay kèm xử lý đồng mức ưu tiên, truy nguyên bằng chứng bản tin gốc và chuỗi 6 ảnh vệ tinh B13 trước sự kiện, tích hợp bộ bấm giờ & chấm điểm tự động KPI so sánh 2 cách xem, và hiển thị bảng đối soát số lượng / Quarantine / thông lượng HDFS).

---

## 1. Mục tiêu và Quyết định Thiết kế Đã Chốt

Qua quá trình thảo luận thiết kế, nhóm thống nhất các quyết định cốt lõi cho Mô-đun 4:

1. **Kiến trúc Giao diện Đa Tab Trọn gói (3 Tab Nghiệp vụ & Kỹ thuật):**
   - **Tab 1 — Bảng Rà soát Quyết định & Truy nguyên Bằng chứng (Decision & Provenance Inspector):**
     - Hiển thị bảng tổng hợp đúng **3 dòng** (`VVDN`, `VVNB`, `VVTS`) cho mốc thời gian rà soát đang chọn (`review_slot_utc`), hiển thị song song cả giờ **UTC (`HH:MMZ`)** và **Giờ Việt Nam (`HH:MM ICT`, UTC+7)**.
     - Nêu bật sân bay thuộc mức ưu tiên cao nhất (`is_top_priority = True`). Khi có nhiều sân bay đồng mức cao nhất (`is_co_priority_top = True`), hiển thị nhãn cảnh báo **"ĐỒNG MỨC ƯU TIÊN ĐẦU (Co-Priority Tie)"** và danh sách các sân bay được chấp nhận làm đáp án đầu tiên (`accepted_first_review_airports`).
     - Hỗ trợ nút/tùy chọn **Đảo thứ tự dòng hiển thị** (`Mức ưu tiên tăng dần`, `Mã sân bay A→Z`, `Mã sân bay Z→A / Đảo thứ tự đồng mức`) để chứng minh trực quan yêu cầu của đề cương: *"Đảo thứ tự hai dòng rồi chạy lại vẫn phải chấp nhận cả hai lựa chọn"*.
     - Khi chọn một sân bay bất kỳ, mở bảng **Truy nguyên Bằng chứng 3 Nguồn (Provenance Drill-Down)**:
       1. Bản tin METAR gốc (`raw_ob`), giờ quan trắc, giờ hệ thống nhận, tuổi dữ liệu (phút), tầm nhìn (mét + cờ giới hạn `9999`), mã hiện tượng tại trạm / lân cận / xu hướng (`TEMPO`/`BECMG`) và phiên bản sửa (`revision_index`).
       2. Phiên bản dự báo Open-Meteo hợp lệ tại thời điểm rà soát (`forecast_fetched_at_utc <= review_slot_utc`), lượng mưa dự báo giờ tới (`forecast_rain_next_hour_mm`), hoặc thông báo khuyết dự báo rõ ràng đối với các ca lịch sử trước thời điểm bật máy thu.
       3. Chuỗi tối đa **6 ảnh vệ tinh Himawari-9 kênh B13 (10 phút/ảnh)** trước sự kiện (`[slot - 50 phút, ..., slot]`) lấy từ `data/processed/himawari_png/airport=<ICAO>/`, kèm bộ chỉ số Dual-Box ($\pm 1.0^\circ$ và $\pm 0.25^\circ$: `b13_valid_fraction`, `inner_bt_min_c`, `inner_cold_cloud_fraction`, `inner_severe_convection_fraction`, `b13_summary_vi`) và dòng ghi công bắt buộc (`Processed Product | Source: JMA/NOAA Himawari-9 B13 via AWS Open Data`).
   - **Tab 2 — Đo lường & Chấm điểm Tự động KPI Thực nghiệm (KPI Timer & Auto-Grader — CLO4):**
     - Phục vụ trực tiếp buổi thử nghiệm với người ngoài nhóm ở Tuần 3 và minh chứng chấm điểm tiêu chí **CLO4 (15% điểm)**.
     - Cung cấp chế độ **3 Nguồn Rời (Baseline View)** ngay trên ứng dụng (hiển thị tách biệt 3 khung dữ liệu thô: bảng METAR chữ thuần, bảng JSON dự báo Open-Meteo, và thư mục ảnh vệ tinh rời mà không có xếp hạng ưu tiên hay gợi ý lý do) bên cạnh chế độ **Dashboard Tổng hợp (Dashboard View)**.
     - Tích hợp **Đồng hồ bấm giờ nhận ca (Start / Stop Timer)** và **Phiếu ghi nhận lượt thử**:
       - Ghi nhận: Mã người thử (`participant_id`), Mã ca (`case_id` gắn với `review_slot_utc`), Cách xem (`view_mode`: `baseline_3_sources` hoặc `dashboard`), Thứ tự thử (`trial_order`: `1` hoặc `2`), Sân bay chọn đầu tiên (`selected_airport`), Lý do chọn (`selected_reason_code`), Giờ bản tin làm căn cứ (`entered_obs_time` — chấp nhận cả định dạng UTC như `07:00Z`/`08:00Z` hoặc giờ Việt Nam quy đổi đúng như `14:00`/`15:00`), và Trạng thái (`COMPLETED` hoặc `ABANDONED`).
       - **Chấm đáp án tự động (Auto-Grader)** độc lập với thứ tự dòng: Lượt thử được tính là **ĐẠT (`PASS`)** khi và chỉ khi:
         1. `selected_airport` nằm trong tập `accepted_first_review_airports` của ca đó,
         2. Lý do chọn khớp với nhóm nguyên nhân của chính sân bay đã chọn,
         3. Giờ bản tin căn cứ khớp với giờ quan trắc METAR của sân bay đã chọn (theo UTC hoặc UTC+7).
       - Lưu bền vững từng lượt thử vào `data/logs/kpi_trials_log.jsonl` (kèm cờ `is_sample_seed` để phân biệt dữ liệu mẫu diễn tập và dữ liệu đo thật) và tự động tính bảng tổng hợp KPI: **Số lượt đạt / Tổng số lượt thử**, **Thời gian trung vị (Median Seconds) của các lượt đạt** theo từng cách xem, và **Mức rút ngắn thời gian (%) đối chiếu với mục tiêu 30%**.
   - **Tab 3 — Đối soát Chất lượng Dữ liệu, Quarantine & Ma trận Lưu trữ (Data Quality & Reconciliation — CLO2/CLO3):**
     - Hiển thị bảng đối soát số lượng tự động từ `data/processed/reconciliation/reconciliation_summary.json`: kiểm chứng đẳng thức `rows_read = rows_valid_pre_dedup + rows_quarantined` và `rows_valid_pre_dedup = rows_retained_valid + rows_duplicate_dropped` cho cả 3 nguồn (`metar`, `open_meteo`, `himawari_metrics`).
     - Trình chiếu danh sách các bản ghi bị tách vào bảng lỗi `data/processed/quarantine/quarantine_records.jsonl` (hiển thị rõ `error_codes`, `input_file_path`, `raw_payload`, `is_synthetic_test`) và nhật ký lỗi tệp nhị phân vệ tinh `data/logs/himawari_file_errors.jsonl`.
     - Tổng hợp số đo thực nghiệm 5V & Ma trận lưu trữ từ `data/logs/ingestion_log.jsonl` (tổng số tệp, tổng dung lượng byte, tỷ lệ lượt quét đủ 2 phân đoạn `S0410` + `S0510`) và `data/logs/hdfs_sync_log.jsonl` (thông lượng đồng bộ Hadoop WebHDFS `throughput_mb_s`, độ trễ `elapsed_ms`).

2. **Cơ chế Nạp dữ liệu Tức thì (< 1 giây) Kết hợp Kích hoạt PySpark Tùy chọn:**
   - Mặc định, Dashboard đọc trực tiếp các tệp JSONL / JSON và ảnh PNG đã được Mô-đun 3 xuất ra trong `data/processed/` và `data/logs/`. Nhờ đó, tốc độ chuyển đổi giữa các mốc thời gian phát lại (`Replay Mode`) đạt dưới 1 giây và hoạt động 100% ngay cả khi ngắt mạng hoàn toàn (Offline Backup Mode khi bảo vệ).
   - Trên thanh công cụ bên trái (Sidebar), cung cấp nút **"Chạy lại Pipeline PySpark (Re-run Spark)"** (kèm tùy chọn bật/tắt `--inject-quarantine-test`) để giảng viên hoặc người chấm có thể kích hoạt trực tiếp `src.spark.run_pipeline` ngay từ giao diện khi cần kiểm tra tính tái lập.

3. **Đóng gói Đa Môi trường (`.venv` Windows & Docker Compose):**
   - Bổ sung `streamlit>=1.32.0` vào `requirements.txt`.
   - Cung cấp kịch bản 1 chạm `run_dashboard.bat` trên Windows và dịch vụ `dashboard` (mở cổng `http://localhost:8501`) trong `docker-compose.yml`.

---

## 2. Kiến trúc Mô-đun và Cấu trúc Tệp (`src/dashboard/`)

```text
DLL/
├── src/
│   └── dashboard/
│       ├── __init__.py
│       ├── data_loader.py         # Đọc & chuẩn hóa múi giờ UTC/VN cho decision_table, quarantine, reconciliation, logs, và tìm chuỗi 6 ảnh B13
│       ├── kpi_evaluator.py       # Chấm đáp án tự động (hỗ trợ đồng mức & quy đổi giờ UTC/VN), ghi/đọc kpi_trials_log.jsonl & tính thống kê Median/Pass Rate
│       └── app.py                 # Ứng dụng giao diện chính Streamlit (Sidebar + 3 Tab nghiệp vụ)
├── tests/
│   └── dashboard/
│       ├── __init__.py
│       ├── test_data_loader.py    # Kiểm thử đọc bảng quyết định, khôi phục nhãn giờ UTC, tìm chuỗi 6 ảnh B13 và xử lý khi thiếu tệp
│       ├── test_kpi_evaluator.py  # Kiểm thử chấm đáp án tự động (Ca 1 đáp án đơn, Ca 2 đồng mức đảo thứ tự, quy đổi giờ UTC/UTC+7, tính Median KPI)
│       └── test_app_smoke.py      # Kiểm thử khởi chạy không lỗi (headless/AppTest) cho giao diện Streamlit
├── data/
│   └── logs/
│       └── kpi_trials_log.jsonl   # Nhật ký lưu kết quả các lượt đo KPI thực nghiệm
└── run_dashboard.bat              # Kịch bản khởi chạy nhanh Dashboard trên Windows (.venv)
```

---

## 3. Chi tiết Kỹ thuật Từng Thành phần

### 3.1. Bộ Đọc Dữ liệu & Chuỗi Ảnh Vệ tinh (`src/dashboard/data_loader.py`)

1. **Chuẩn hóa Mốc Thời gian Rà soát (`normalize_utc_Slot`):**
   - Khi PySpark trên Windows xuất `TimestampType` qua `.collect()`, các chuỗi ISO không có hậu tố `Z` trong `decision_table.jsonl` (ví dụ `"2026-11-09T14:00:00"`) có thể đang ở giờ địa phương (`UTC+7`) hoặc `UTC`.
   - Hàm `parse_and_normalize_utc(ts_str: str, raw_ob: str | None = None) -> dict` sẽ đối chiếu với mã ngày-giờ trong `raw_ob` (ví dụ `090700Z` $\rightarrow$ `07:00 UTC` = `14:00 ICT`) hoặc chuẩn hóa nhất quán về:
     - `utc_iso`: `"2026-11-09T07:00:00Z"`
     - `utc_hhmm`: `"07:00Z"`
     - `vn_iso`: `"2026-11-09 14:00:00 ICT (UTC+7)"`
     - `vn_hhmm`: `"14:00"`
   - Điều này đảm bảo cả điều phối viên xem giờ Việt Nam (`14:00` / `15:00`) lẫn người đối chiếu mã METAR gốc (`090700Z` / `090800Z`) đều thấy thông tin chính xác tuyệt đối.

2. **Hàm `load_decision_slots(storage_root: str = "data") -> dict[str, list[dict]]`:**
   - Đọc `data/processed/decision_table/decision_table.jsonl`.
   - Nhóm các dòng theo mốc rà soát chuẩn hóa `slot_utc_iso` (ví dụ `"2026-11-09T07:00:00Z"` và `"2026-11-09T08:00:00Z"`).
   - Hỗ trợ hàm `sort_slot_rows(rows: list[dict], sort_mode: str) -> list[dict]` với 3 chế độ:
     - `"priority"`: Sắp xếp theo `(priority_level ASC, airport_icao ASC)`.
     - `"icao_asc"`: Sắp xếp theo `airport_icao ASC` (`VVDN`, `VVNB`, `VVTS`).
     - `"icao_desc"`: Sắp xếp đảo ngược `airport_icao DESC` (`VVTS`, `VVNB`, `VVDN`) — đảo vị trí giữa `VVNB` và `VVDN` trong ca đồng mức.

3. **Hàm `get_b13_image_sequence(storage_root: str, airport_icao: str, scan_time_utc_iso: str, max_frames: int = 6) -> list[dict]`:**
   - Tìm trong thư mục `data/processed/himawari_png/airport=<airport_icao>/` tất cả các tệp ảnh `B13_<ICAO>_<YYYYMMDD>_<HHMM>Z.png` có thời điểm quét $\le$ `scan_time_utc_iso` (trong cửa sổ 60 phút trước sự kiện).
   - Sắp xếp tăng dần theo thời gian quét và lấy tối đa `max_frames = 6` ảnh gần nhất (ví dụ chuỗi `07:00Z`, `07:10Z`, `07:20Z`, `07:30Z`, `07:40Z`, `07:50Z`, `08:00Z`) để hiển thị thanh trượt diễn biến mây (Cloud Evolution Time-Lapse) trên Dashboard.

4. **Hàm `load_reconciliation_and_quarantine(storage_root: str = "data") -> dict`:**
   - Đọc `data/processed/reconciliation/reconciliation_summary.json`, `data/processed/quarantine/quarantine_records.jsonl`, `data/logs/himawari_file_errors.jsonl`, `data/logs/ingestion_log.jsonl`, và `data/logs/hdfs_sync_log.jsonl`.
   - Nếu `quarantine_records.jsonl` hiện tại đang rỗng (do lần chạy cuối không bật cờ `--inject-quarantine-test`), cung cấp thêm bản xem trước các bản ghi lỗi mẫu từ `data/raw/synthetic_quarantine/` hoặc cho phép người dùng bấm nút chạy lại kèm `--inject-quarantine-test`.

---

### 3.2. Bộ Chấm Điểm Tự động & Thống kê KPI (`src/dashboard/kpi_evaluator.py`)

1. **Quy tắc Chấm Đáp án Tự động (`evaluate_kpi_submission`):**
   - Đầu vào:
     - `slot_rows: list[dict]` (3 dòng sân bay của ca đang chấm).
     - `selected_airport: str` (`"VVDN"`, `"VVNB"`, hoặc `"VVTS"`).
     - `selected_reason_code: str` (ví dụ `"PRECIP_OR_THUNDER_AT_STATION"`, `"LOW_VISIBILITY"`, `"STALE_OR_MISSING_METAR"`, `"FORECAST_RAIN_NEXT_HOUR"`, `"ROUTINE"`).
     - `entered_obs_time: str` (chuỗi người dùng nhập, ví dụ `"07:00Z"`, `"0700Z"`, `"090700Z"`, `"07:00"`, hoặc giờ VN `"14:00"`, `"14h00"`).
     - `elapsed_seconds: float`, `status: str` (`"COMPLETED"` hoặc `"ABANDONED"`).
   - Logic kiểm tra:
     - Nếu `status != "COMPLETED"`: trả về `is_passed = False`, `failure_reason = "ABANDONED"`.
     - Kiểm tra 1 (**Đúng sân bay mức ưu tiên cao nhất**): `selected_airport` phải nằm trong `accepted_first_review_airports` của ca đó. (Với ca đồng mức `08:00Z`, cả `VVDN` và `VVNB` đều hợp lệ, bất kể bảng đang sắp xếp theo thứ tự nào).
     - Kiểm tra 2 (**Đúng lý do tại sân bay đã chọn**): Tìm dòng của `selected_airport` trong `slot_rows`, đối chiếu `selected_reason_code` với điều kiện thực tế của sân bay đó (ví dụ tại `VVDN` hoặc `VVNB` lúc `08:00Z` là mưa/dông tại trạm `"PRECIP_OR_THUNDER_AT_STATION"`).
     - Kiểm tra 3 (**Đúng giờ bản tin căn cứ theo UTC hoặc Giờ Việt Nam**): Chuẩn hóa `entered_obs_time` và so khớp với `utc_hhmm` (ví dụ `"07:00"` / `"090700Z"`), hoặc `vn_hhmm` (`"14:00"`).
     - Nếu thỏa cả 3 điều kiện $\rightarrow$ `is_passed = True`, ngược lại `is_passed = False` kèm mã lỗi chi tiết (`WRONG_AIRPORT`, `WRONG_REASON`, `WRONG_OBS_TIME`).

2. **Tính Toán Chỉ số Tổng hợp KPI (`compute_kpi_summary`):**
   - Nhóm các bản ghi trong `data/logs/kpi_trials_log.jsonl` theo `view_mode` (`"baseline_3_sources"` vs `"dashboard"`).
   - Với mỗi cách xem, tính:
     - `total_trials`: Tổng số lượt thử.
     - `passed_trials`: Số lượt đạt (`is_passed == True`).
     - `abandoned_trials`: Số lượt bỏ dở.
     - `pass_rate_pct`: `passed_trials / total_trials * 100`.
     - `median_passed_seconds`: Trung vị (`statistics.median`) của `elapsed_seconds` chỉ tính trên các lượt **ĐẠT (`is_passed == True`)** (đúng theo quy định tại Mục 1 đề cương: *"báo cáo thời gian trung vị của lượt đạt cùng số lượt đạt trên tổng số lượt thử"*).
   - Tính mức cải thiện:
     $$\text{time\_reduction\_pct} = \frac{\text{median\_baseline} - \text{median\_dashboard}}{\text{median\_baseline}} \times 100\%$$
     và đối chiếu với ngưỡng mục tiêu đề xuất **30%**.

---

## 4. Tiêu chuẩn Kiểm thử & Nghiệm thu (Acceptance Criteria)

1. **Kiểm thử Đơn vị cho `data_loader.py` (`tests/dashboard/test_data_loader.py`):**
   - Đọc chính xác 2 ca phát lại từ `data/processed/decision_table/decision_table.jsonl`:
     - Ca `2026-11-09T07:00:00Z` (`14:00 ICT`): 1 đáp án đầu (`["VVDN"]`).
     - Ca `2026-11-09T08:00:00Z` (`15:00 ICT`): 2 đáp án đồng mức đầu (`["VVDN", "VVNB"]`).
   - Hàm `sort_slot_rows` đảo thứ tự dòng (`icao_desc`) nhưng không làm thay đổi `accepted_first_review_airports`.
   - Hàm `get_b13_image_sequence` trả về đúng chuỗi tối đa 6 ảnh PNG theo thứ tự thời gian tăng dần và không lấy ảnh tương lai (`> scan_time_utc`).

2. **Kiểm thử Đơn vị cho `kpi_evaluator.py` (`tests/dashboard/test_kpi_evaluator.py`):**
   - Chấm đúng Ca 1 (`07:00Z`): Chọn `VVDN` + mưa tại trạm + giờ `07:00Z` hoặc `14:00` $\rightarrow$ `PASS`; chọn `VVNB` $\rightarrow$ `FAIL (WRONG_AIRPORT)`.
   - Chấm đúng Ca 2 đồng mức (`08:00Z`): Chọn `VVDN` (kèm giờ `08:00Z`/`15:00`) HOẶC chọn `VVNB` (kèm giờ `08:00Z`/`15:00`) đều trả về `PASS`, kể cả khi danh sách dòng đầu vào đã bị đảo thứ tự.
   - Tính đúng trung vị thời gian của các lượt đạt (`median_passed_seconds`) và tỷ lệ giảm thời gian (`time_reduction_pct`).

3. **Kiểm thử Khởi chạy Giao diện (`tests/dashboard/test_app_smoke.py`):**
   - Render ứng dụng Streamlit bằng `streamlit.testing.v1.AppTest` (hoặc kiểm tra hàm điều phối giao diện) đảm bảo không phát sinh bất kỳ ngoại lệ nào khi chuyển đổi giữa các Tab và các mốc phát lại.

