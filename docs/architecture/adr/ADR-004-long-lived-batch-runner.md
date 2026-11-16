# ADR-004: Runner Spark chạy lâu dài, đọc batch theo lịch; không dùng Structured Streaming trên Bronze hiện tại

- Trạng thái: Đã chấp nhận (2026-11-16)

## Bối cảnh
- `RawEnvelopeStore` ghi mỗi batch bằng cách viết lại nguyên tử chính shard cuối (`raw_store.py:283-296`); tên tệp không đổi.
- File source của Structured Streaming chỉ theo dõi **đường dẫn mới**. Nó sẽ bỏ sót mọi bản ghi nối vào shard đã đọc.
- Số đo B5 (`.harness/2026-11-16-architecture/logs/`) cho thấy JVM khởi động **không phải** nút thắt:

| Đo | Trước | Sau |
|---|---|---|
| Khởi tạo SparkSession (lạnh) | 5,7 s | 4,5 s |
| Biến đổi + ghép + quy tắc | 181–197 s | 14,2 s (lạnh), 6,0 s (ấm) |
| Tổng một lần chạy (131–134 mốc) | 228 s (lạnh), 218 s (ấm) | 52 s (lạnh), 18 s (ấm) |

- Nguyên nhân thật: khung mốc (spine) được tạo bằng `spark.createDataFrame(list)`. Đó là một Python RDD, nên **mỗi stage** đọc spine lại khởi động tiến trình Python worker (rất chậm trên Windows). Trên dữ liệu tổng hợp 3 mốc, thời gian `count()` giảm từ hơn 3 phút xuống 1,8 s sau khi dựng spine bằng literal trong JVM (`temporal_join.py`).

## Quyết định
1. Spine và các bảng nhỏ được dựng bằng biểu thức trong JVM (`spark.range(1).select(F.explode(F.array(...)))`, `F.inline`). Mã pipeline không dùng `createDataFrame(list)`.
2. `python -m src.spark.run_pipeline --loop` (`src/spark/runner.py`):
   - Giữ một SparkSession duy nhất.
   - Mỗi `--poll-seconds` (mặc định 15 s) kiểm tra chữ ký Bronze (`RawEnvelopeStore.signature()`) và chạy lại pipeline khi có thay đổi.
   - Sau mỗi chu kỳ gọi `spark.catalog.clearCache()`.
   - Ghi heartbeat vào `data/lake/_runner_heartbeat.json`.
3. Chỉ được chuyển sang Structured Streaming khi đáp ứng đủ 3 điều kiện:
   - Bronze bất biến: mỗi lần flush là một tệp mới.
   - Sink `foreachBatch` idempotent, khóa theo `batch_id` và `review_slot_utc`.
   - Có test khởi động lại từ checkpoint, chứng minh không mất và không trùng dòng.

## Hệ quả
- Độ trễ công bố khi runner chạy bằng một chu kỳ ấm (khoảng 18 s) cộng thời gian chờ poll.
- Runner lỗi không làm dừng hệ thống: trạng thái `ERROR` hiện trong heartbeat, và tab "Sức khỏe hệ thống" báo heartbeat quá hạn.
