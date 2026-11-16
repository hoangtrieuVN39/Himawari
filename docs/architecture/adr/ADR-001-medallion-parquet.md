# ADR-001: Tổ chức dữ liệu theo Medallion, Silver/Gold lưu Parquet có phiên bản

- Trạng thái: Đã chấp nhận (2026-11-16)
- Liên quan: ADR-002, ADR-003

## Bối cảnh
- Trước thay đổi, pipeline `collect()` mọi kết quả về driver rồi ghi JSONL bằng `pathlib.Path` (`run_pipeline.py` cũ, dòng 194–222). Không có Parquet ở bất kỳ đâu trong `src/`, dù kế hoạch nhóm ghi "bảng quyết định Parquet".
- Bảng hợp lệ (Silver) không được lưu: mỗi phân tích phải đọc lại và kiểm tra lại toàn bộ JSONL thô.
- Ghi đè `decision_table.jsonl` tại chỗ khiến người đọc có thể thấy tệp đang ghi dở.

## Quyết định
1. Ba lớp dữ liệu:
   - **Bronze**: giữ nguyên định dạng nguồn (`data/raw_compact/<nguồn>/date=YYYY-MM-DD/batches-NNNN.jsonl`, `.DAT.bz2`, PNG).
   - **Silver** (Parquet): `metar_observations`, `open_meteo_forecasts`, `himawari_b13_metrics` (đã kiểm tra schema, loại trùng, phân vùng `date=`) và `quarantine`.
   - **Gold** (Parquet): `decision_table` (phân vùng `review_date=`), `reconciliation`, `verification`, `feature_table`, `nowcast`, `health/`.
2. Mỗi bảng là **chuỗi phiên bản bất biến**: `<bảng>/run_id=<YYYYMMDDTHHMMSSffffffZ>/...`. Một phiên bản chỉ hiển thị với người đọc khi Spark đã ghi `_SUCCESS`. Sau khi commit, giữ 2 phiên bản mới nhất và xóa thư mục ghi dở cũ hơn (`src/spark/writers.py`).
3. Danh mục bảng và quy tắc đường dẫn nằm tại một chỗ duy nhất: `src/spark/lake_paths.py`.

## Hệ quả
- Người đọc (dashboard, chẩn đoán, ML) luôn thấy một snapshot đầy đủ, kể cả khi pipeline đang chạy. Không cần khóa giữa người đọc và Spark.
- Dùng chế độ ghi `static` cho từng phiên bản. Chế độ `partitionOverwriteMode=dynamic` không ghi `_SUCCESS` ở gốc thư mục nên bị loại (đã kiểm chứng trên Spark 3.5).
- Mỗi lần chạy viết lại toàn bộ Silver/Gold (khoảng 1,5 MB với dữ liệu hiện tại). Khi dữ liệu lớn hơn nhiều, cần chuyển sang ghi tăng dần theo phân vùng (xem `04-roadmap.md`).
- Không cần Delta/Iceberg ở quy mô này. Mẫu "phiên bản + `_SUCCESS`" cho tính nguyên tử đủ dùng mà không thêm phụ thuộc.
