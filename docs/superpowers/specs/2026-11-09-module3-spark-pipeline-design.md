# Tài liệu Thiết kế (Spec) — Mô-đun 3: Pipeline PySpark, Quarantine, Chuẩn hóa Nghiệp vụ, Ghép Thời gian & Quy tắc Ưu tiên

**Ngày tạo:** 2026-11-09  
**Dự án:** Hỗ trợ điều phối viên rà soát thời tiết tại ba sân bay Việt Nam (`VVDN`, `VVNB`, `VVTS`)  
**Phạm vi:** Mô-đun 3 (Mốc Tuần 2 · Buổi 4–6 & Kiểm thử Nghiệm thu Kỹ thuật Buổi 8 — Xây dựng pipeline PySpark đọc dữ liệu theo schema chặt chẽ từ kho cục bộ hoặc Hadoop HDFS, tách lỗi vào bảng `quarantine`, chuẩn hóa tầm nhìn và hiện tượng METAR, ghép theo thời điểm không rò rỉ tương lai, xếp hạng ưu tiên Level 1–4 kèm xử lý đồng mức, xuất bảng quyết định Parquet và bảng đối soát số lượng).

---

## 1. Mục tiêu và Quyết định Thiết kế Đã Chốt

Qua quá trình thảo luận thiết kế, nhóm thống nhất các quyết định kỹ thuật và nghiệp vụ cốt lõi cho Mô-đun 3:

1. **Môi trường Thực thi PySpark Đóng gói trong Docker (`spark-processor`):**
   - Toàn bộ quá trình chạy pipeline PySpark và bộ kiểm thử `pytest` của Mô-đun 3 được thực thi bên trong Docker container (`spark-processor`) tích hợp sẵn `OpenJDK 17`, `pyspark>=3.5.0`, và `pyarrow>=15.0.0`.
   - Cách tiếp cận này loại bỏ hoàn toàn xung đột `winutils.exe` / `HADOOP_HOME` trên Windows cá nhân và đảm bảo tính tái lập 100% khi thành viên khác chạy kiểm tra chéo ở Buổi 7–8.
   - Hỗ trợ đọc/ghi linh hoạt từ kho tệp cục bộ (`/app/data`) hoặc trực tiếp từ cụm Hadoop HDFS (`hdfs://namenode:9000/data`) qua tham số `--storage-root`.

2. **Kiểm soát Schema & Tách lỗi Quarantine Có Truy nguyên:**
   - Định nghĩa `StructType` tường minh cho cả 3 nguồn (`metar`, `open_meteo`, `himawari_metrics`).
   - Mọi bản ghi sai kiểu dữ liệu (`_corrupt_record`), thiếu khóa bắt buộc, sân bay ngoài danh sách `["VVDN", "VVNB", "VVTS"]`, thời gian quan trắc không hợp lệ, hoặc giá trị vật lý ngoài miền hợp lệ (như nhiệt độ sáng kênh B13 ngoài $[150.0\text{ K}, 340.0\text{ K}]$, tỷ lệ mây ngoài $[0.0, 1.0]$, tầm nhìn hoặc lượng mưa âm) đều bị tách vào bảng `data/processed/quarantine/` kèm mảng mã lỗi `error_codes`, đường dẫn tệp gốc `input_file_path` và cờ `is_synthetic_test`.
   - Hỗ trợ cờ `--inject-quarantine-test` để sinh dữ liệu lỗi có gắn nhãn `"is_synthetic_test": true` phục vụ trình diễn trực tiếp ở Buổi 6 và Buổi 8.

3. **Chuẩn hóa Khí tượng METAR Chính xác:**
   - **Tầm nhìn (`visibility_m`):** Trích xuất tầm nhìn hệ mét từ bản tin gốc `rawOb` (nhóm 4 chữ số hệ mét hoặc `CAVOK`) kết hợp trường `visib` (statute miles). Khi gặp mã `9999` hoặc `CAVOK` hoặc `"6+"`, chuẩn hóa `visibility_m = 10000` (hoặc `9999`) và bật cờ `visibility_capped_9999 = True`. Đánh dấu `is_low_visibility = True` khi `visibility_m < 5000`.
   - **Phân tách hiện tượng thời tiết:** Tách phần thân quan trắc chính (trước `TEMPO` / `BECMG` / `NOSIG` / `RMK`) khỏi phần dự báo xu hướng (`TEMPO` / `BECMG`). Phân biệt rõ 3 nhóm:
     1. **Mưa/dông tại trạm (`has_precip_or_thunder_at_station = True`):** `TS`, `TSRA`, `RA`, `-RA`, `+RA`, `SHRA`, `-SHRA`, `+SHRA`, `DZ`, `SQ`, `GR`... xuất hiện ở phần thân chính và không có tiền tố `VC`.
     2. **Hiện tượng lân cận (`has_vicinity_weather = True`):** `VCTS`, `VCSH`, `VCFG`, `VCRA`... xuất hiện ở phần thân chính.
     3. **Hiện tượng trong phần xu hướng (`trend_weather_codes`):** xuất hiện sau `TEMPO` hoặc `BECMG`.

4. **Ghép theo Thời điểm (Point-in-Time Temporal Join — Chống Rò rỉ Tương lai):**
   - Với mỗi thời điểm rà soát `review_slot_utc` (`as_of_utc`), luôn khởi tạo khung xương cố định gồm đúng **3 dòng sân bay** (`VVDN`, `VVNB`, `VVTS`) để đảm bảo mỗi lượt rà soát luôn xuất ra đúng 3 khóa `(review_slot_utc, airport_icao)` kể cả khi thiếu nguồn.
   - **Quy tắc chống rò rỉ tương lai:**
     - Ở chế độ vận hành trực tiếp (`--mode live`), chỉ những bản ghi có thời điểm sự kiện $\le$ `review_slot_utc` **VÀ** thời điểm hệ thống nhận `fetched_at_utc <= review_slot_utc` mới được phép tham gia ghép.
     - Ở chế độ phát lại lịch sử (`--mode replay`), cho phép ghép các bản ghi có cờ `is_backfill = True` theo thời điểm sự kiện (`observed_at_utc <= review_slot_utc` / `scan_time_utc <= review_slot_utc`), nhưng **vẫn chặn nghiêm ngặt** mọi bản dự báo hoặc bản tin sửa đến sau `review_slot_utc` đối với các bản ghi không phải backfill hoặc trong các ca kiểm thử dữ liệu đến trễ.
   - **Xử lý bản tin được sửa (`COR` / `SPECI`):** Nếu cùng một `(airport_icao, observed_at_utc)` có bản tin gốc và bản tin sửa, tại thời điểm `review_slot_utc` chỉ chọn phiên bản mới nhất đã nhận tính đến `review_slot_utc` (`fetched_at_utc <= review_slot_utc`), đồng thời lưu bảng lịch sử phiên bản.

5. **Quy tắc Xếp hạng Ưu tiên (Level 1–4) & Xử lý Đồng mức (Co-Priority):**
   - **Mức 1 (`priority_level = 1` — Khẩn cấp tại trạm):** METAR còn mới (`metar_age_minutes <= 60`) VÀ (`has_precip_or_thunder_at_station == True` HOẶC `visibility_m < 5000`).
   - **Mức 2 (`priority_level = 2` — Cần xác minh dữ liệu):** Không thuộc Mức 1, VÀ bản tin METAR bị thiếu HOẶC đã cũ quá 60 phút (`metar_age_minutes > 60`).
   - **Mức 3 (`priority_level = 3` — Cảnh báo dự báo giờ tới):** METAR còn mới ($\le 60$ phút), điều kiện tại trạm bình thường, NHƯNG lượng mưa dự báo giờ tới từ Open-Meteo `forecast_rain_next_hour_mm >= 0.1` mm/h.
   - **Mức 4 (`priority_level = 4` — Theo dõi thường quy):** Các trạm còn lại (METAR còn mới, tầm nhìn $\ge 5000\text{ m}$, không mưa/dông tại trạm, dự báo mưa giờ tới $< 0.1\text{ mm/h}$).
   - **Đồng mức (`is_co_priority_top`):** Mọi sân bay cùng đạt `min(priority_level)` trong lượt rà soát đều có `is_top_priority = True`. Nếu có từ 2 sân bay trở lên cùng đạt mức cao nhất, tất cả đều mang cờ `is_co_priority_top = True`, hiển thị lý do và giờ bản tin căn cứ riêng của từng sân bay, và tập đáp án chuẩn `accepted_top_airports` chứa tất cả các sân bay đó độc lập với thứ tự dòng.

---

## 2. Kiến trúc Mô-đun 3 và Cấu trúc Thư mục

```text
DLL/
├── Dockerfile                                 # Bổ sung openjdk-17-jre-headless, pyspark, pyarrow, pandas
├── docker-compose.yml                         # Bổ sung dịch vụ spark-processor kết nối namenode/datanode
├── requirements.txt                           # Bổ sung pyspark>=3.5.0, pyarrow>=15.0.0, pandas>=2.2.0
├── src/
│   └── spark/
│       ├── __init__.py
│       ├── session.py                         # Khởi tạo SparkSession (local[*], UTC timezone, HDFS/local config)
│       ├── schemas.py                         # StructType cho METAR, Open-Meteo, Himawari B13 & bảng đầu ra
│       ├── readers.py                         # Đọc JSON envelopes, explode mảng con, gắn batch_id & input_file_path
│       ├── quarantine.py                      # Kiểm tra chất lượng, tách bảng quarantine, loại trùng & bảng đối soát
│       ├── transforms.py                      # Chuẩn hóa tầm nhìn (9999/SM->m), tách hiện tượng METAR & nhãn mây B13
│       ├── temporal_join.py                   # Ghép theo thời điểm as_of_utc (chống rò rỉ tương lai, chọn bản sửa COR/SPECI)
│       ├── priority_engine.py                 # Quy tắc xếp hạng Level 1-4, xử lý đồng mức & tạo câu giải thích tiếng Việt
│       └── run_pipeline.py                    # CLI chạy trọn pipeline (--review-slots, --mode, --inject-quarantine-test)
├── tests/
│   └── spark/
│       ├── conftest.py                        # Fixture SparkSession dùng chung cho toàn bộ test suite
│       ├── test_schemas_and_readers.py        # Test đọc JSON 3 nguồn & bắt dòng sai schema (_corrupt_record)
│       ├── test_quarantine_and_reconcile.py   # Test tách lỗi vật lý/sân bay lạ & công thức đối soát bất biến
│       ├── test_transforms.py                 # Test chuẩn hóa tầm nhìn 9999/SM, tách mưa tại trạm vs VCTS vs TEMPO
│       ├── test_temporal_join.py              # Test chống rò rỉ tương lai (future forecast leakage) & bản tin sửa COR
│       ├── test_priority_engine.py            # Test 2 ca nghiệp vụ đã chốt (ca 1 đáp án & ca đồng mức đảo thứ tự dòng)
│       └── test_run_pipeline.py               # Test tích hợp đầu-cuối và tính tái lập (chạy 2 lần cùng số khóa)
└── data/
    └── processed/
        ├── valid_metar/                       # Bảng METAR hợp lệ đã chuẩn hóa & đánh số phiên bản (Parquet)
        ├── valid_open_meteo/                  # Bảng các phiên bản dự báo hợp lệ theo giờ (Parquet)
        ├── valid_himawari_metrics/            # Bảng chỉ số vệ tinh B13 Dual-Box hợp lệ (Parquet)
        ├── quarantine/                        # Bảng các bản ghi lỗi kèm error_codes & input_file_path (Parquet + JSONL)
        ├── data_profile/                      # Thống kê chất lượng dữ liệu theo nguồn & sân bay (JSON + Parquet)
        ├── reconciliation/                    # Bảng đối soát số lượng dòng đọc = hợp lệ + lỗi (JSON + Parquet)
        └── decision_table/                    # Bảng quyết định 3 dòng/lượt rà soát phục vụ Streamlit (Parquet + JSONL)
```

---

## 3. Thiết kế Chi tiết 5 Bước Pipeline PySpark

### 3.1. Bước 1 — Đọc với Schema (`src/spark/schemas.py` & `src/spark/readers.py`)

PySpark đọc các tệp phong bì JSON từ `data/raw/{metar,open_meteo,himawari_metrics}/date=*/` ở chế độ `PERMISSIVE` kèm cột `columnNameOfCorruptRecord="_corrupt_record"` và `multiLine=True`:

1. **Nguồn METAR (`read_raw_metar_envelopes`):**
   - Trích xuất từ `metadata`: `batch_id`, `source`, `fetched_at_utc`, `is_backfill`.
   - Dùng `posexplode_outer("payload")` để tách từng phần tử quan trắc sân bay:
     - `airport_icao` (`icaoId`), `obs_time_epoch` (`obsTime`), `report_time_raw` (`reportTime`), `receipt_time_raw` (`receiptTime`), `raw_ob` (`rawOb`), `visib_raw` (`visib`), `wx_string_raw` (`wxString`), `temp_c` (`temp`), `dewp_c` (`dewp`), `wdir_deg` (`wdir`), `wspd_kt` (`wspd`), `wgst_kt` (`wgst`), `altim_hpa` (`altim`), `flt_cat` (`fltCat`).
   - Quy đổi `observed_at_utc`: ưu tiên chuyển từ `obs_time_epoch` (epoch giây UTC) sang `TimestampType`; nếu thiếu mới parse từ `report_time_raw`.

2. **Nguồn Dự báo Open-Meteo (`read_raw_open_meteo_envelopes`):**
   - Trích xuất từ `metadata`: `batch_id`, `source`, `airport_icao`, `fetched_at_utc`, `is_backfill`.
   - Dùng `arrays_zip` trên `payload.hourly.{time, precipitation, rain, showers, weather_code, visibility, wind_speed_10m, wind_gusts_10m}` rồi `explode` thành từng dòng dự báo theo giờ hiệu lực:
     - `forecast_valid_at_utc` (parse từ chuỗi ISO `"YYYY-MM-DDTHH:MM"` theo múi giờ UTC), `precipitation_mm`, `rain_mm`, `showers_mm`, `weather_code`, `forecast_visibility_m`, `wind_speed_10m_kmh`, `wind_gusts_10m_kmh`.

3. **Nguồn Chỉ số Vệ tinh Himawari-9 B13 (`read_raw_himawari_metrics_envelopes`):**
   - Trích xuất từ `metadata`: `batch_id`, `source`, `scan_slot_utc`, `processed_at_utc`, `is_backfill`, `is_synthetic_test`.
   - Dùng `posexplode_outer("airports")` để tách từng dòng chỉ số theo sân bay:
     - `airport_icao`, `segment_code`, `scan_time_utc`, `fetched_at_utc`, `status`, `png_rel_path`, `b13_valid_fraction`, `outer_bt_min_k`, `outer_bt_min_c`, `outer_cold_cloud_fraction`, `outer_severe_convection_fraction`, `inner_bt_min_k`, `inner_bt_min_c`, `inner_cold_cloud_fraction`, `inner_severe_convection_fraction`.

### 3.2. Bước 2 — Kiểm tra Chất lượng, Tách lỗi Quarantine, Loại trùng & Đối soát (`src/spark/quarantine.py`)

#### Quy tắc Kiểm tra và Mã lỗi (`error_codes: ArrayType(StringType)`)

| Nguồn | Điều kiện vi phạm | Mã lỗi (`error_code`) |
|---|---|---|
| Cả 3 nguồn | `_corrupt_record IS NOT NULL` hoặc thiếu `batch_id` / `fetched_at_utc` | `SCHEMA_VIOLATION` |
| Cả 3 nguồn | `airport_icao` bị null hoặc $\notin \{\text{"VVDN"}, \text{"VVNB"}, \text{"VVTS"}\}$ | `UNKNOWN_AIRPORT` |
| Cả 3 nguồn | Không chuyển đổi được timestamp sự kiện (`observed_at_utc` / `forecast_valid_at_utc` / `scan_time_utc`) hoặc `fetched_at_utc` | `INVALID_TIMESTAMP` |
| METAR | `observed_at_utc > fetched_at_utc + INTERVAL 10 MINUTES` (khi `is_backfill == False`) | `FUTURE_OBSERVATION_TIME` |
| METAR | `raw_ob` bị null/rỗng hoặc độ dài $< 10$ ký tự, hoặc tầm nhìn quy đổi $< 0$ | `INVALID_METAR_PAYLOAD` |
| Open-Meteo | `precipitation_mm < 0` hoặc `rain_mm < 0` hoặc `showers_mm < 0` hoặc `forecast_visibility_m < 0` | `NEGATIVE_FORECAST_VALUE` |
| Himawari B13 | `outer_bt_min_k` hoặc `inner_bt_min_k` nằm ngoài $[150.0\text{ K}, 340.0\text{ K}]$ | `OUT_OF_RANGE_BT_KELVIN` |
| Himawari B13 | `b13_valid_fraction`, `inner_cold_cloud_fraction`, hoặc `inner_severe_convection_fraction` ngoài $[0.0, 1.0]$ | `OUT_OF_RANGE_CLOUD_FRACTION` |

- Mọi bản ghi có `size(error_codes) > 0` được chuyển sang DataFrame `quarantine_df` với cấu trúc chuẩn:
  `quarantine_id`, `source`, `batch_id`, `airport_icao`, `event_time_raw`, `fetched_at_raw`, `error_codes`, `raw_record_json`, `input_file_path`, `is_synthetic_test`, `quarantined_at_utc`.

#### Loại trùng & Giữ lịch sử Phiên bản (Deduplication & Versioning)
- **METAR hợp lệ:**
  - Nhiều chu kỳ thu 10 phút/lần có thể trả về cùng một bản tin METAR không đổi (`airport_icao`, `observed_at_utc`, `raw_ob` trùng hoàn toàn). Spark nhóm theo `(airport_icao, observed_at_utc, raw_ob)` và chỉ giữ bản ghi có `fetched_at_utc` sớm nhất (các bản ghi lặp hoàn toàn được đếm vào `rows_duplicate_dropped`).
  - Nếu cùng `(airport_icao, observed_at_utc)` nhưng `raw_ob` khác nhau (bản tin sửa `COR` hoặc `SPECI`), cả hai phiên bản đều được giữ lại trong `valid_metar` và đánh số thứ tự `revision_index = 1, 2, ...` theo `fetched_at_utc` tăng dần.
- **Open-Meteo hợp lệ:**
  - Loại bỏ các dòng trùng hoàn toàn trên khóa `(airport_icao, fetched_at_utc, forecast_valid_at_utc)`, giữ nguyên tất cả các phiên bản dự báo có `fetched_at_utc` khác nhau.
- **Himawari B13 hợp lệ:**
  - Nhóm theo `(airport_icao, scan_time_utc)`, giữ bản ghi hợp lệ có `fetched_at_utc` mới nhất, các bản lặp bị đếm vào `rows_duplicate_dropped`.

#### Bảng Đối soát Số lượng Bất biến (`reconciliation`)
Với mỗi nguồn $s \in \{\text{metar}, \text{open\_meteo}, \text{himawari\_metrics}\}$, bảng đối soát ghi nhận và kiểm tra tự động 2 đẳng thức bất biến:
$$\text{rows\_read}_s = \text{rows\_valid\_pre\_dedup}_s + \text{rows\_quarantined}_s$$
$$\text{rows\_valid\_pre\_dedup}_s = \text{rows\_retained\_valid}_s + \text{rows\_duplicate\_dropped}_s$$
Đồng thời tổng hợp riêng số tệp nhị phân `.DAT.bz2` lỗi từ `data/logs/himawari_file_errors.jsonl`.

### 3.3. Bước 3 — Biến đổi Nghiệp vụ (`src/spark/transforms.py`)

1. **Chuẩn hóa Tầm nhìn METAR (`normalize_visibility_meters`):**
   - Tìm nhóm tầm nhìn hệ mét 4 chữ số trong phần thân chính của `raw_ob` (đứng ngay sau nhóm gió `...KT` hoặc biến thiên hướng gió `...V...`, ví dụ `10011G23KT 9999` hoặc `05006KT 2000` hoặc `CAVOK`):
     - Nếu có `CAVOK` hoặc `9999` $\rightarrow$ `visibility_m = 10000`, `visibility_capped_9999 = True`.
     - Nếu có nhóm 4 chữ số `dddd` ($0000 \le dddd < 9999$) $\rightarrow$ `visibility_m = int(dddd)`, `visibility_capped_9999 = False`.
   - Nếu không có trong `raw_ob`, quy đổi từ `visib_raw` (statute miles):
     - `"6+"` hoặc `"10+"` $\rightarrow$ `visibility_m = 10000`, `visibility_capped_9999 = True`.
     - Giá trị số `v` (ví dụ `"3"`, `"1.5"`) $\rightarrow$ `visibility_m = int(round(float(v) * 1609.34))`, `visibility_capped_9999 = False`.
   - Tính cờ `is_low_visibility = (visibility_m < 5000)`.

2. **Phân tích Hiện tượng Thời tiết trong METAR (`parse_metar_weather`):**
   - Cắt `raw_ob` tại từ khóa xu hướng đầu tiên (`\b(TEMPO|BECMG|NOSIG|RMK)\b`):
     - Phần trước là `main_body` (quan trắc thực tế tại thời điểm `observed_at_utc`).
     - Phần sau là `trend_section` (dự báo xu hướng ngắn hạn trong METAR).
   - Quét các token trong `main_body`:
     - **Mưa/dông tại trạm (`has_precip_or_thunder_at_station`):** Khớp biểu thức chính quy `^(\+|-)?(TS|SH|FZ)?(RA|DZ|SN|GR|GS|PL|SQ|FC|TS)+$` (không bắt đầu bằng `VC`). Ví dụ: `RA`, `-RA`, `+RA`, `SHRA`, `-SHRA`, `+SHRA`, `TS`, `TSRA`, `+TSRA`, `-TSRA`, `DZ`.
     - **Hiện tượng lân cận (`has_vicinity_weather`):** Khớp `^VC(TS|SH|FG|RA|PO|BL|DS|SS)+$` (ví dụ `VCTS`, `VCSH`, `VCFG`).
     - **Hiện tượng mù/sương mù giảm tầm nhìn tại trạm (`obscuration_codes`):** `FG`, `BR`, `HZ`.
   - Quét các token trong `trend_section`:
     - Lưu vào `trend_weather_codes` (ví dụ `TEMPO 3000 TSRA` $\rightarrow$ `trend_weather_codes = ["TSRA"]`, nhưng `has_precip_or_thunder_at_station = False` nếu phần thân chính không có mưa/dông).

3. **Phân loại Mức Cảnh báo Đỉnh Mây B13 (`classify_b13_cloud_state`):**
   - Dựa trên chỉ số vùng trong tại trạm ($\pm 0.25^\circ$):
     - Nếu `inner_severe_convection_fraction >= 0.05` (hoặc `inner_bt_min_k < 220.0`) $\rightarrow$ `b13_cloud_tag = "SEVERE_CONVECTION_AT_STATION"` (Đỉnh mây dông mạnh $< -53.15^\circ\text{C}$ ngay trên sân bay).
     - Ngược lại nếu `inner_cold_cloud_fraction >= 0.05` (hoặc `inner_bt_min_k < 235.0`) $\rightarrow$ `b13_cloud_tag = "COLD_CONVECTIVE_CLOUD_AT_STATION"` (Mây đối lưu lạnh $< -38.15^\circ\text{C}$ tại trạm).
     - Ngược lại nếu `outer_cold_cloud_fraction >= 0.05` $\rightarrow$ `b13_cloud_tag = "COLD_CLOUD_IN_OUTER_VICINITY"` (Có mây lạnh trong vùng lân cận $\pm 1.0^\circ$).
     - Ngược lại $\rightarrow$ `b13_cloud_tag = "WARM_OR_CLEAR_CLOUD"`.

### 3.4. Bước 4 — Ghép theo Thời điểm & Quy tắc Ưu tiên Level 1–4 (`src/spark/temporal_join.py` & `src/spark/priority_engine.py`)

#### Ghép theo Thời điểm (`build_point_in_time_airport_snapshot`)
Cho mỗi mốc rà soát `review_slot_utc`:
1. Tạo bảng khung `spine_df` gồm 3 dòng: `(review_slot_utc, "VVDN")`, `(review_slot_utc, "VVNB")`, `(review_slot_utc, "VVTS")`.
2. **Chọn METAR hiệu lực tại `review_slot_utc`:**
   - Lọc các bản ghi `valid_metar` có `observed_at_utc <= review_slot_utc` và thỏa điều kiện nhận:
     - Chế độ `live` (hoặc bản ghi không phải backfill): `fetched_at_utc <= review_slot_utc`.
     - Chế độ `replay`: `fetched_at_utc <= review_slot_utc` HOẶC (`is_backfill == True` VÀ không có bản tin sửa nào nhận trước `review_slot_utc`).
   - Xếp hạng theo `Window.partitionBy("airport_icao").orderBy(col("observed_at_utc").desc(), col("fetched_at_utc").desc(), col("revision_index").desc())` và lấy dòng `row_number() == 1`.
   - Tính `metar_age_minutes = round((unix_timestamp(review_slot_utc) - unix_timestamp(observed_at_utc)) / 60.0, 1)`.
3. **Chọn Dự báo Open-Meteo hiệu lực tại `review_slot_utc`:**
   - Lọc các bản dự báo có `fetched_at_utc <= review_slot_utc` (nếu chạy chế độ `replay` cho ca lịch sử mà mọi bản dự báo đều thu sau ca đó, chỉ khi bật cờ `allow_backfill_forecast=True` mới lấy bản dự báo đầu tiên và gắn cờ `forecast_missing_prior_version = True`; mặc định kiểm thử thời gian luôn chặn cứng `fetched_at_utc <= review_slot_utc`).
   - Với mỗi sân bay, chọn phiên bản dự báo (`batch_id`) có `fetched_at_utc` lớn nhất $\le$ `review_slot_utc`.
   - Lấy dòng dự báo ứng với giờ kế tiếp `target_forecast_hour_utc` (khung giờ tròn tiếp theo sau `review_slot_utc`, ví dụ rà soát lúc `07:00Z` hoặc `07:30Z` lấy dự báo của `08:00Z`), tính `forecast_rain_next_hour_mm = greatest(precipitation_mm, rain_mm + showers_mm)`.
4. **Chọn Ảnh & Chỉ số Vệ tinh Himawari-9 B13 tại `review_slot_utc`:**
   - Lọc các lượt quét có `scan_time_utc <= review_slot_utc` (và `fetched_at_utc <= review_slot_utc` khi không phải backfill).
   - Lấy lượt quét mới nhất để điền chỉ số B13 hiện tại và đường dẫn `latest_b13_png_path`.
   - Thu thập tối đa 6 lượt quét trong khoảng `[review_slot_utc - 50 minutes, review_slot_utc]` sắp xếp tăng dần theo `scan_time_utc` để tạo mảng `pre_event_b13_png_paths` (chuỗi 6 ảnh trước sự kiện).

#### Ma trận Quy tắc Ưu tiên Rà soát (Level 1–4)

| Mức ưu tiên (`priority_level`) | Nhãn mức (`priority_label`) | Điều kiện kích hoạt | Mã lý do (`reason_codes`) & Giải thích hiển thị |
|---|---|---|---|
| **1 (Cao nhất)** | `LEVEL_1_HAZARD_AT_STATION` | `metar_is_present` VÀ `metar_age_minutes <= 60` VÀ (`has_precip_or_thunder_at_station == True` HOẶC `visibility_m < 5000`) | `AT_STATION_PRECIP_OR_TS` và/hoặc `LOW_VISIBILITY_LT_5KM`. Ví dụ: *"METAR mới (07:00Z) ghi nhận mưa/dông tại trạm (-RA), tầm nhìn 9999m"* hoặc *"METAR mới ghi mưa, tầm nhìn thấp 2000m (< 5km)"*. |
| **2** | `LEVEL_2_DATA_VERIFICATION` | Không thuộc Mức 1 VÀ (`NOT metar_is_present` HOẶC `metar_age_minutes > 60`) | `METAR_MISSING` hoặc `METAR_STALE_GT_60M`. Ví dụ: *"Cần xác minh dữ liệu: Bản tin METAR đã cũ 90 phút (> 60 phút)"*. |
| **3** | `LEVEL_3_FORECAST_RAIN_ALERT` | Không thuộc Mức 1–2 VÀ `forecast_is_present` VÀ `forecast_rain_next_hour_mm >= 0.1` | `FORECAST_RAIN_NEXT_HOUR`. Ví dụ: *"Điều kiện tại trạm tốt, nhưng dự báo mưa giờ tới đạt 2.4 mm (hiệu lực 16:00Z)"*. |
| **4 (Thấp nhất)** | `LEVEL_4_ROUTINE` | Các trường hợp còn lại (METAR mới $\le 60$ phút, không mưa/dông tại trạm, tầm nhìn $\ge 5000\text{ m}$, dự báo mưa giờ tới $< 0.1\text{ mm}$) | `NORMAL_CONDITIONS`. Ví dụ: *"Điều kiện tại trạm bình thường, không có cảnh báo mưa giờ tới"*. |

#### Xử lý Đồng mức (`Co-Priority Tie Handling`)
- Trên mỗi `review_slot_utc`:
  - `top_priority_level = min(priority_level)` trên 3 sân bay.
  - Sân bay có `priority_level == top_priority_level` được gắn `is_top_priority = True`.
  - `co_priority_top_count` = số sân bay có `is_top_priority == True`.
  - `is_co_priority_top = ( is_top_priority AND co_priority_top_count > 1 )`.
  - `accepted_first_review_airports` = danh sách đã sắp xếp chữ cái của mọi `airport_icao` có `is_top_priority == True` (ví dụ `["VVDN"]` ở ca 1 đáp án, hoặc `["VVDN", "VVNB"]` ở ca đồng mức).

---

## 4. Kế hoạch Kiểm thử & Đối soát Nghiệm thu (Buổi 6 & Buổi 8)

1. **Kiểm thử Đối soát Số lượng (`test_quarantine_and_reconcile.py`):**
   - Nạp bộ dữ liệu mẫu gồm cả bản ghi hợp lệ, bản ghi trùng lặp và bản ghi lỗi (`--inject-quarantine-test`).
   - Xác minh tự động:
     - `rows_read == rows_valid_pre_dedup + rows_quarantined` cho cả 3 nguồn.
     - `rows_valid_pre_dedup == rows_retained_valid + rows_duplicate_dropped`.
     - Mọi bản ghi lỗi đều có `error_codes` chính xác và `input_file_path` truy nguyên được.
2. **Kiểm thử Thời gian & Bản tin Sửa (`test_temporal_join.py`):**
   - **Chặn rò rỉ tương lai:** Tại mốc rà soát `15:00Z`, một bản dự báo mưa lớn có `fetched_at_utc = 15:15Z` tuyệt đối không được xuất hiện trong kết quả lúc `15:00Z`, chỉ xuất hiện khi rà soát ở `15:20Z`.
   - **Bản tin METAR được sửa (`COR` / `SPECI`):** Bản tin gốc lúc `15:00Z` (nhận lúc `15:02Z`) báo thời tiết tốt, bản tin sửa `COR` cho cùng mốc `15:00Z` (nhận lúc `15:08Z`) báo mưa dông `TSRA`. Khi rà soát ở `15:05Z` kết quả dùng bản gốc; khi rà soát ở `15:10Z` kết quả tự động nâng lên Mức 1 theo bản sửa `COR`.
3. **Kiểm thử 2 Ca Nghiệp vụ Đã Chốt (`test_priority_engine.py`):**
   - **Ca 1 (Một đáp án):** `VVDN` có METAR mới ghi dông kèm mưa (`TSRA`) $\rightarrow$ Mức 1; `VVNB` có METAR cũ 90 phút $\rightarrow$ Mức 2 (`METAR_STALE_GT_60M`); `VVTS` có METAR mới thời tiết tốt + dự báo mưa giờ tới $1.5\text{ mm}$ $\rightarrow$ Mức 3. Kết quả: `accepted_first_review_airports == ["VVDN"]`, `is_co_priority_top == False`.
   - **Ca 2 (Đồng mức đầu):** `VVDN` có METAR mới ghi dông (`TS`), tầm nhìn $6000\text{ m}$ $\rightarrow$ Mức 1; `VVNB` có METAR mới ghi mưa (`RA`), tầm nhìn $2000\text{ m}$ $\rightarrow$ Mức 1; `VVTS` bình thường $\rightarrow$ Mức 4. Kết quả: Cả `VVDN` và `VVNB` đều có `priority_level == 1`, `is_top_priority == True`, `is_co_priority_top == True`, `accepted_first_review_airports == ["VVDN", "VVNB"]`. Đảo thứ tự dòng đầu vào rồi chạy lại vẫn cho kết quả bất biến.
4. **Kiểm thử Đầu ra & Tái lập (`test_run_pipeline.py`):**
   - Mỗi `review_slot_utc` luôn sinh đúng 3 dòng `(review_slot_utc, airport_icao)` kể cả khi 1 sân bay bị thiếu hoàn toàn dữ liệu.
   - Chạy pipeline 2 lần liên tiếp trên bộ mẫu thực tế (`07:00Z` và `08:00Z` ngày `2026-11-09`) cho ra cùng số khóa, cùng giá trị quyết định và cùng số bản ghi lỗi.

