# ADR-003: Spark là nơi duy nhất tính mức ưu tiên, dashboard chỉ đọc Gold

- Trạng thái: Đã chấp nhận (2026-11-16)

## Bối cảnh
- `src/dashboard/data_loader.py` cũ có hai đường tính song song:
  - Spark tính các mốc replay.
  - Ở chế độ LIVE, dashboard tự tính lại bằng Python thuần (`_compute_slot_rows_from_raw`, khoảng 230 dòng) rồi ghi đè `decision_table.jsonl` mà Spark tạo ra.
- Hai đường chỉ dùng chung `priority_rules.py`. Phần ghép theo thời điểm, loại trùng, chọn bản sửa và kiểm tra lỗi được viết hai lần. Đường Python không đi qua quarantine nên không đối soát được.
- Hai đường cùng ghi một tệp. Phải dùng khóa `decision_table.lock`, và vẫn còn rủi ro hai cách hiểu khác nhau về cùng một mốc.

## Quyết định
- Xóa đường tính bằng Python (`_compute_slot_rows_from_raw`, `sync_latest_raw_to_decision_table`, `_SYNC_CACHE`).
- Dashboard chỉ đọc phiên bản Gold `decision_table` mới nhất đã commit (`load_decision_slots`).
- "Cập nhật realtime" chạy ingestion, rồi công bố Gold mới theo một trong hai cách:
  - Nếu Spark runner chạy lâu dài (ADR-004) còn hoạt động (heartbeat mới), chờ runner commit phiên bản mới.
  - Nếu không, chạy `python -m src.spark.run_pipeline` một lần.
- Bộ theo dõi LIVE so sánh phiên bản Gold đang hiển thị với phiên bản mới nhất (`has_new_decision_version`).

## Hệ quả
- Mọi dòng hiển thị đều đã qua quarantine và có số đối soát.
- Một lần cập nhật thủ công khi không có runner mất khoảng 20–60 s (Spark), thay vì dưới 1 s bằng Python. Khi chạy runner, độ trễ là thời gian một chu kỳ Spark (khoảng 18 s khi JVM đã khởi động, đo ở B5).
- Test `tests/dashboard/test_live_sync.py` được viết lại theo hợp đồng mới. Các test chỉ kiểm tra đường Python đã xóa thì bỏ đi.
