# 04 — Lộ trình mở rộng

Mỗi nấc chỉ được kích hoạt khi điều kiện đo được xảy ra. Chi phí ở đây là ước tính, chưa phải số đo, trừ những chỗ ghi rõ.

| Nấc | Điều kiện kích hoạt | Thay đổi | Chi phí / rủi ro |
|---|---|---|---|
| 0 (hiện tại) | 3 sân bay; 30 ngày METAR; Himawari 816 MB/ngày thô (đo B8), xóa tệp thô sau khi đã trích chỉ số | Lake Parquet có phiên bản; runner Spark chạy lâu dài; HDFS single-node là tùy chọn | Một máy cá nhân. Áp lực bộ nhớ và đĩa là giới hạn thật (ADR-005) |
| 1 — Ghi tăng dần | Một lần chạy ấm vượt 60 s, hoặc Silver vượt 1 GB | Chỉ ghi lại các phân vùng `date=` bị ảnh hưởng (hôm nay, hôm qua); phiên bản hóa theo phân vùng | Cần chỉ mục phiên bản theo phân vùng; tăng độ phức tạp của GC |
| 2 — Object storage dùng chung | Nhiều máy hoặc nhiều người cùng đọc lake; giữ dữ liệu lâu hơn 90 ngày | Chuyển `--lake-root` sang S3/MinIO (`s3a://`). Mã không đổi nhờ Hadoop FileSystem API (ADR-002) | Thêm dịch vụ MinIO, hoặc phí lưu trữ cloud |
| 3 — HDFS nhiều DataNode | Cần xử lý tại chỗ trên cụm, bắt buộc `dfs.replication ≥ 2` | Thêm DataNode, chạy Spark trong mạng Compose (`spark-processor`) | Mỗi JVM DataNode cần khoảng 1 GB RAM |
| 4 — Spark standalone/cluster | Tải bù vượt khả năng một máy (ví dụ 30 ngày Himawari mất khoảng 10,7 h ở p=4 theo B8) | Chạy `himawari_batch` trên cụm; phân vùng đầu vào theo ngày | Cần đồng bộ môi trường Satpy trên mọi worker |
| 5 — Luồng sự kiện và orchestrator | Hơn 20 sân bay, nhiều nhóm tiêu thụ, hoặc từ 3 job phụ thuộc có SLA (ADR-005) | Bronze bất biến kết hợp Structured Streaming `foreachBatch` (đủ 3 điều kiện của ADR-004); có thể thêm Airflow | Thêm vận hành; phải có test khởi động lại |

## Lộ trình nghiệp vụ
- Thu `precipitation_probability` của Open-Meteo từ một ngày cố định, để chấm dự báo bằng Brier và reliability.
- Đánh giá cùng điều phối viên thực tế trước khi đề xuất triển khai, đo lại KPI chính.
- Mở rộng nhãn cho mô hình dự báo 0–60′ khi có thêm mùa mưa: hiện 383 giờ dương tính, tập trung ở VVDN.
