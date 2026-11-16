# 02 — Đánh giá kiến trúc (có số đo)

Số đo lấy trên máy nhóm: Windows 11, 12 lõi, 16 GB RAM; commit charge 45,9–46,8 GB trên giới hạn 49,4 GB; ổ E: còn khoảng 226 MB trống. Log gốc nằm trong `.harness/2026-11-16-architecture/logs/`. Những gì chưa đo được ghi là **giả thuyết** hoặc **định tính**.

## 1. 5V

| V | Số đo / quan sát | Nguồn |
|---|---|---|
| Volume | Himawari: **5,67 MB tải về mỗi lượt quét** cho 2 phân đoạn (kế hoạch ước 2,5 MB), tức khoảng 816 MB/ngày ở 144 lượt. METAR 30 ngày tải bù: 4.341 bản ghi, 2,18 MB. Open-Meteo khoảng 4 KB mỗi sân bay mỗi giờ. Lake Silver+Gold mỗi phiên bản khoảng 1,5 MB. Ngoại suy 30 ngày Himawari: 24,5 GB tải về, khoảng 26 h xử lý tuần tự | B8 (`b8_satpy.json`), A2, `du data/lake` |
| Velocity | Ba nhịp khác nhau: METAR 10′ (2 h look-back), Open-Meteo 60′, Himawari 10′. Ảnh có trên S3 sau khoảng 15–20′ (daemon xử lý mốc 05:50 lúc 06:11) | `ingest_daemon.log`, `gold/health` |
| Variety | Nhị phân `.DAT.bz2` (HSD), PNG, JSON lồng (METAR, dự báo theo mảng giờ), Parquet | Bronze/Lake |
| Veracity | Trùng hoàn toàn trong Bronze: **57% METAR** (do look-back 2 h) và **76% Open-Meteo**. 6 phân đoạn thực sự thiếu trên S3 (`SOURCE_MISSING`: 2026-11-15 14:40Z; 2026-11-16 02:40Z, 04:40Z). Daemon ngừng chạy 11-11→11-15 (`RECEIVER_FAULT`). `inner_bt_min_c` null ở 2/45 dòng. AWC không trả SPECI cho 3 trạm | `src/diagnostics`, BackfillIngest |
| Value | Nhãn cho mô hình 0–60′: 383 giờ-sân bay dương tính trên 2.132 giờ có quan trắc (18,0%). VVDN 185, VVNB 113, VVTS 85 | A2 |

## 2. Điểm mạnh, điểm yếu theo tầng

| Tầng | Phát hiện | Mức độ | Trạng thái |
|---|---|---|---|
| Thu nhận | Ghi nguyên tử và khóa liên tiến trình tốt. Idempotent theo `batch_id` | Mạnh | Giữ nguyên |
| Thu nhận | Tệp thô `.DAT.bz2` không bao giờ bị xóa sau migration (khoảng 816 MB/ngày) | **Cao**: ổ E: chỉ còn 226 MB | Đã sửa `cleanup_old_raw_segments` (có test hồi quy); daemon chạy với `--retain-raw-hours 2` |
| Thu nhận | Ảnh thay thế khi thiếu mốc bị gắn nhãn thời gian của mốc yêu cầu | **Cao**: bằng chứng sai thời điểm | Đã sửa: giữ đúng `source_slot_utc` |
| Bronze | Shard bị viết lại tại chỗ, không tương thích với file-streaming | Trung bình | Ghi trong ADR-004; dùng runner batch |
| Spark | Mỗi lần chạy khoảng 3,5 phút. Nguyên nhân là spine dựng bằng Python RDD, không phải JVM | **Cao** | Đã sửa: 228 s → 52 s (lạnh), 218 s → 18 s (ấm) |
| Spark | Không có Parquet; không ghi được lên HDFS | Cao | Đã sửa (ADR-001, ADR-002) |
| Spark | Dòng spine bị mất khi mọi METAR đều sau mốc; replay nhận dự báo lấy về muộn | Cao (sai hợp đồng "đúng 3 khóa", rò rỉ tương lai) | Đã sửa và có test (`tests/spark/test_temporal_contracts.py`) |
| Phục vụ | Hai đường tính mức ưu tiên (Spark và Python trong dashboard) | Cao | Đã sửa (ADR-003) |
| Vận hành | Không có cách nào thấy daemon đã ngừng nhiều ngày | Cao | Tab "Sức khỏe hệ thống" và `gold/health` |
| Môi trường | Bộ nhớ và đĩa gần cạn; OpenBLAS và JVM lỗi cấp phát khi chạy song song | Cao (rủi ro demo) | Đặt `OPENBLAS_NUM_THREADS=1`; tải bù Himawari chạy trên ổ D: |

## 3. Nút thắt hiệu năng (B5, B8)

**Pipeline Spark** (`pipeline_runs.jsonl`, `b5_*.txt`):

| Giai đoạn | Trước (131 mốc, lạnh / ấm) | Sau (134 mốc, lạnh / ấm) |
|---|---|---|
| Khởi tạo SparkSession | 5,7 s / 0,0 s | 4,5 s / 0,0 s |
| Đọc + kiểm tra + quarantine | 9,2 s / 3,2 s | 11,9 s / 3,0 s |
| Biến đổi + ghép + quy tắc | **181,0 s / 196,5 s** | **14,2 s / 6,0 s** |
| Ghi lake | 23,9 s / 16,7 s | 13,4 s / 7,7 s |
| Tổng | 228 s / 218 s | 52 s / 18 s |

Trên dữ liệu tổng hợp 3 mốc, `count()` mất hơn 3 phút trước khi sửa và 1,8 s sau khi sửa. Thời gian lập kế hoạch truy vấn chỉ khoảng 0,5 s, nên giả thuyết "JVM khởi động là nút thắt" bị **bác bỏ**. Nguyên nhân là mỗi stage đọc Python RDD đều phải khởi động Python worker.

**Satpy** (6 mốc, trung vị của 3 lần):

| Chế độ | Thời gian | Lượt quét/phút | So với tuần tự |
|---|---|---|---|
| Tuần tự | 130 s | 2,77 | 1× |
| Spark p=2 | 89 s | 4,05 | 1,46× |
| Spark p=4 | 53 s | 6,7 | 2,44× |

- Một mốc ở trạng thái lạnh mất 19,1 s: 2,2 s import, 6,9 s tải, 8,8 s xử lý. RSS đỉnh khoảng 410 MB.

## 4. Ma trận lưu trữ

| Tiêu chí | Kho tệp cục bộ + Parquet có phiên bản (chính) | HDFS single-node (Dual-Mode, tùy chọn) | PostgreSQL (so sánh) |
|---|---|---|---|
| Workload | Lưu được cả nhị phân, PNG, JSON và Parquet. Spark quét theo phân vùng `date=` | Giống cột trái, đọc qua `hdfs://` | Mạnh với tra cứu điểm và cập nhật từng dòng. Không hợp với `.DAT.bz2` và PNG (định tính) |
| Độ trễ ghi (đo) | Commit 6 bảng: 7,7 s (ấm) | WebHDFS: 0,86 MB/s cho 49 tệp/16,6 MB (một lần đo). Kết quả B6 ở mục 4.1 | Chưa đo |
| Độ trễ đọc (đo) | Dashboard nạp Gold dưới 1 s (`test_replay_load_is_fast`) | Không phục vụ dashboard (ADR-002) | Định tính: tra cứu có chỉ mục nhanh |
| Mở rộng | Một máy; chuyển sang `s3a://` không cần sửa mã | Thêm DataNode, tăng `dfs.replication` | Phân vùng/sharding phải tự thiết kế |
| Quản trị | `_SUCCESS` làm dấu commit, giữ 2 phiên bản, quarantine, đối soát | `permissions.enabled=false`, `replication=1`: chưa đảm bảo an toàn hay bền vững | ACID, phân quyền, ràng buộc khóa |
| Chi phí | Khoảng 0 MB RAM nền; lake khoảng 1,5 MB mỗi phiên bản | RAM container: xem mục 4.1 | Khoảng 200–500 MB RAM nền (định tính) |

**Kết luận:** kho tệp cục bộ cộng Parquet có phiên bản là lớp lưu trữ chính. HDFS giữ vai trò minh họa xử lý phân tán và ghi lake từ Spark (ADR-002), nhưng không nằm trên đường găng của demo.

### 4.1 B6 — WebHDFS: chi phí theo tệp và theo byte
Chưa có số đo. Mục này được cập nhật sau khi chạy `.harness/2026-11-16-architecture/b6_hdfs_bench.py` (cùng 16 MB, chia thành 1/10/100 tệp, mỗi cấu hình 3 lần).
