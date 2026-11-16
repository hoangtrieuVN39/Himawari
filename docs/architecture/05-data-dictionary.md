# 05 — Từ điển dữ liệu (Silver/Gold)

Mỗi trường được mô tả bằng sáu thuộc tính: **tên, kiểu, ý nghĩa/đơn vị, nguồn, quy tắc chất lượng, mức nhạy cảm**.
- Mọi timestamp đều ở UTC (`timestamp[us, tz=UTC]` trong Parquet).
- Toàn bộ dữ liệu là dữ liệu khí tượng công khai, nên mức nhạy cảm là **Công khai**, trừ các trường ghi khác.
- Schema chính xác của từng phiên bản đọc được bằng `pyarrow.parquet.read_schema` trên `data/lake/<tầng>/<bảng>/run_id=.../`.

## 1. Thời điểm sự kiện và thời điểm nhận dữ liệu

| Nguồn | Thời điểm sự kiện | Thời điểm nhận (hệ thống) | Thời điểm nguồn nhận |
|---|---|---|---|
| METAR | `observed_at_utc` | `fetched_at_utc` | `receipt_time_utc` (AWC) |
| Open-Meteo | `forecast_valid_at_utc` (cuối khoảng tích lũy 1 h) | `fetched_at_utc` (= giờ phát hành phiên bản dự báo mà hệ thống có) | — |
| Himawari B13 | `scan_time_utc` | `fetched_at_utc` / `processed_at_utc` | — |

Ghép theo thời điểm: một bản ghi chỉ được dùng cho mốc `t` khi thời điểm sự kiện ≤ `t`. Ở chế độ `live` còn cần thời điểm nhận ≤ `t`. Riêng dự báo luôn cần `fetched_at_utc ≤ t` ở **mọi** chế độ.

## 2. Silver `metar_observations` (phân vùng `date` = ngày của `observed_at_utc`)

| Tên | Kiểu | Ý nghĩa / đơn vị | Nguồn | Quy tắc chất lượng |
|---|---|---|---|---|
| `airport_icao` | string | Mã ICAO | AWC `icaoId` | ∈ {VVDN, VVNB, VVTS}, nếu không thì vào quarantine `UNKNOWN_AIRPORT` |
| `observed_at_utc` | timestamp | Giờ quan trắc | `obsTime` (epoch), dự phòng `reportTime` | Không null (`INVALID_TIMESTAMP`). Bản ghi không phải tải bù thì không được muộn hơn `fetched_at_utc` quá 10′ |
| `fetched_at_utc` | timestamp | Giờ hệ thống lấy về | metadata envelope | Không null (`SCHEMA_VIOLATION`) |
| `is_backfill` | bool | Bản ghi tải bù (`--backfill-days`) | metadata | Mặc định false |
| `raw_ob` | string | Bản tin gốc | `rawOb` | Loại bản tin `NIL`; trùng hoàn toàn (sân bay, giờ, raw_ob) thì chỉ giữ 1 |
| `revision_index` | int | Thứ tự bản sửa của cùng một quan trắc (1 = gốc, 2 = COR…) | Tính trong Spark | `dense_rank` theo `fetched_at_utc` |
| `visibility_m` | int | Tầm nhìn, đơn vị mét | Phân tích `raw_ob`, dự phòng `visib` (dặm × 1609,34) | 9999/CAVOK → 10000 kèm cờ `visibility_capped_9999` |
| `is_low_visibility` | bool | `visibility_m < 5000` | Tính | — |
| `has_precip_or_thunder_at_station` | bool | Có mưa hoặc dông tại trạm (không tính VC) | Regex trên phần thân bản tin | — |
| `station_weather_codes` / `vicinity_weather_codes` / `trend_weather_codes` | array<string> | Mã hiện tượng tại trạm / lân cận / phần TEMPO-BECMG | Regex | — |
| `input_file_path` | string | Shard Bronze chứa bản ghi | Spark | Dùng để truy nguyên |

## 3. Silver `open_meteo_forecasts` (phân vùng `date` = ngày của `fetched_at_utc`)

| Tên | Kiểu | Ý nghĩa / đơn vị | Quy tắc chất lượng |
|---|---|---|---|
| `forecast_valid_at_utc` | timestamp | Giờ hiệu lực T; lượng mưa là **tổng của giờ liền trước** `(T−1h, T]` | Theo tài liệu Open-Meteo, biến hourly `precipitation` |
| `precipitation_mm`, `rain_mm`, `showers_mm` | double | mm trong giờ liền trước | ≥ 0, nếu không thì vào quarantine |
| `fetched_at_utc` | timestamp | Giờ phát hành phiên bản (giữ mọi phiên bản) | Không tải bù lịch sử: dự báo thiếu thì để thiếu |

## 4. Silver `himawari_b13_metrics` (phân vùng `date` = ngày của `scan_time_utc`)

| Tên | Kiểu | Ý nghĩa / đơn vị | Quy tắc chất lượng |
|---|---|---|---|
| `scan_time_utc` | timestamp | Giờ quét thật của ảnh (ảnh thay thế khi thiếu mốc giữ đúng giờ của nó) | — |
| `segment_code` | string | `S0410` (VVDN, VVNB) / `S0510` (VVTS) | Lấy từ `config/airports.json` |
| `b13_valid_fraction` | double | Tỷ lệ điểm ảnh có Tb ∈ [150 K, 340 K] trong khung ±1,0° | ∈ [0, 1] |
| `inner_bt_min_k` / `inner_bt_min_c` | double | Tb cực tiểu trong khung ±0,25° (K / °C) | ∈ [150, 350] K |
| `inner_cold_cloud_fraction` | double | Tỷ lệ Tb < 235 K (−38,15 °C) trong khung ±0,25° | ∈ [0, 1] |
| `inner_severe_convection_fraction` | double | Tỷ lệ Tb < 220 K (−53,15 °C) | ∈ [0, 1] |
| `b13_cloud_tag`, `b13_summary_vi` | string | Phân loại mây và mô tả tiếng Việt | Tính trong Spark |
| `png_rel_path` | string | Ảnh PNG đã cắt và chú giải (sản phẩm đã xử lý) | — |

## 5. Silver `quarantine`

| Tên | Kiểu | Ý nghĩa |
|---|---|---|
| `quarantine_id` | string | `<nguồn>_<batch>_<id>` |
| `source`, `batch_id`, `airport_icao` | string | Truy nguyên về đầu vào |
| `error_codes` | array<string> | `SCHEMA_VIOLATION`, `UNKNOWN_AIRPORT`, `INVALID_TIMESTAMP`, `FUTURE_OBSERVATION_TIME`, `INVALID_METAR_PAYLOAD`, `NEGATIVE_FORECAST_VALUE`, `OUT_OF_RANGE_BT_KELVIN`, `OUT_OF_RANGE_CLOUD_FRACTION` |
| `raw_record_json`, `input_file_path` | string | Bản ghi gốc và shard chứa nó |
| `is_synthetic_test` | bool | Bản ghi lỗi do `--inject-quarantine-test` sinh ra |

## 6. Gold `decision_table` (phân vùng `review_date`)

| Tên | Kiểu | Ý nghĩa | Quy tắc chất lượng |
|---|---|---|---|
| `review_slot_utc`, `airport_icao` | timestamp, string | Khóa: mỗi mốc có **đúng 3 dòng** | Spine cố định, kể cả khi thiếu nguồn |
| `slot_kind` | string | `replay` (cấu hình) / `live` (giờ lấy METAR làm tròn lên 5′) | — |
| `metar_is_present`, `forecast_is_present`, `b13_is_present` | bool | Nguồn có bản ghi dùng được tại mốc | Không null |
| `metar_age_minutes` | double | `review_slot_utc − observed_at_utc` (phút) | > 60 thì Mức 2 |
| `target_forecast_hour_utc`, `forecast_rain_next_hour_mm` | timestamp, double | Giờ đích `floor(t)+1h`; `max(precipitation, rain+showers)` | Phiên bản có `fetched_at ≤ t` |
| `priority_level`, `priority_label`, `reason_vi` | int, string | Mức 1–4 và lý do tiếng Việt | `src/spark/priority_rules.py` |
| `accepted_first_review_airports`, `co_priority_top_count`, `is_co_priority_top` | array, int, bool | Đáp án đồng mức của mốc | Không phụ thuộc thứ tự hiển thị |
| `inputs_complete_at_utc` | timestamp | Giờ lấy về muộn nhất trong 3 nguồn | — |
| `published_at_utc`, `first_published_at_utc`, `run_id` | timestamp, string | Lần công bố hiện tại / lần đầu (giữ qua các lần chạy lại) | — |

## 7. Gold `reconciliation`

Mỗi dòng là một nguồn của một lần chạy:
- `rows_read`, `rows_quarantined`, `rows_valid_pre_dedup`, `rows_retained_valid`, `rows_duplicate_dropped`, `is_reconciled`;
- `all_sources_reconciled`;
- `binary_errors_total`, `binary_errors_synthetic`.

Bất biến: `rows_read == rows_valid_pre_dedup + rows_quarantined` và `rows_valid_pre_dedup == rows_retained_valid + rows_duplicate_dropped`.

## 8. Log vận hành (`data/logs/`)

| Tệp | Nội dung | Mức nhạy cảm |
|---|---|---|
| `ingestion_log.jsonl` | Mỗi lần gọi nguồn: trạng thái, số bản ghi, byte; cửa sổ tải bù | Công khai |
| `pipeline_runs.jsonl` | Mỗi lần chạy Spark: `timings_s` theo giai đoạn, `latency_s`, số dòng | Công khai |
| `backfill_log.jsonl` | Tải bù Himawari: trạng thái và thời gian từng mốc | Công khai |
| `himawari_file_errors.jsonl` | Tệp `.DAT.bz2` hỏng, cờ `is_synthetic_test` | Công khai |
| `kpi_trials_log.jsonl` | Lượt đo KPI: mã người thử, thời gian, kết quả | **Nội bộ**, không đưa vào bộ mẫu nộp |
