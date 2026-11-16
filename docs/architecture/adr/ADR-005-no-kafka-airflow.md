# ADR-005: Không dùng Kafka/Airflow ở quy mô hiện tại

- Trạng thái: Đã chấp nhận (2026-11-16)

## Bối cảnh (số đo)
- Nhịp dữ liệu: METAR 10′, Open-Meteo 60′, Himawari 10′.
- Dung lượng Himawari: 5,67 MB mỗi lượt quét cho 2 phân đoạn (B8), khoảng 816 MB/ngày. Toàn bộ METAR và Open-Meteo dưới 1 MB/ngày.
- Bộ nhớ máy chạy: commit charge 45,9–46,8 GB trên giới hạn 49,4 GB. Đã có lỗi `OpenBLAS memory allocation` và JVM `malloc failed` khi chạy song song nhiều tác vụ.
- Đĩa: ổ E: (chứa dự án) còn khoảng 226 MB trống.

## Quyết định
- Không thêm message broker (Kafka) hay orchestrator (Airflow). Lịch chạy giữ nguyên: daemon Python thu nhận, runner Spark (ADR-004), Compose cho tái lập.
- Ngưỡng để xem xét lại, ghi trong `04-roadmap.md`:
  - Hơn 20 sân bay.
  - Hoặc thời gian giữ dữ liệu lớn hơn 90 ngày.
  - Hoặc nhiều nhóm tiêu thụ cần đọc lại luồng sự kiện độc lập.
  - Hoặc từ 3 job phụ thuộc nhau trở lên cần lịch chạy lại có SLA.

## Hệ quả
- Ít dịch vụ JVM hơn, không làm tăng áp lực bộ nhớ.
- Việc chạy bù và thử lại nằm trong mã (`--backfill-days`, `himawari_batch --start/--end`, thao tác idempotent theo `batch_id`), không dựa vào scheduler.
