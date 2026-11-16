# ADR-002: Một writer chung cho kho cục bộ và HDFS (Dual-Mode)

- Trạng thái: Đã chấp nhận (2026-11-16)
- Liên quan: ADR-001

## Bối cảnh
- Thiết kế của nhóm là Dual-Mode: kho tệp cục bộ cộng HDFS single-node (`apache/hadoop:3`, `dfs.replication=1`).
- Trước thay đổi, `--storage-root hdfs://...` hỏng ở cả hai chiều:
  - Chiều đọc: `RawEnvelopeStore` dùng `pathlib.Path`.
  - Chiều ghi: `Path(storage_root) / "processed"`.
- HDFS chỉ nhận dữ liệu qua đồng bộ WebHDFS. Spark chưa bao giờ đọc hay ghi trực tiếp lên HDFS.

## Quyết định
1. `run_pipeline` nhận hai đường dẫn:
   - `--storage-root` (Bronze): thư mục cục bộ hoặc `hdfs://namenode:9000/data`. Với HDFS, Spark đọc `raw_compact/<nguồn>/date=*/*.jsonl` mà `--sync-hdfs` đã đẩy lên.
   - `--lake-root` (Silver/Gold): cục bộ hoặc `hdfs://`. Mặc định là `<storage-root>/lake`.
2. Mọi thao tác hệ tệp của lake (liệt kê phiên bản, kiểm tra `_SUCCESS`, xóa phiên bản cũ) đi qua Hadoop FileSystem API của SparkSession. Cùng một mã chạy được trên đĩa cục bộ và trên HDFS.
3. Đường phục vụ (dashboard) luôn đọc lake **cục bộ** `data/lake` bằng pyarrow. HDFS dành cho xử lý theo lô và dữ liệu lịch sử lớn.
4. Trên Windows, `session.py` tự trỏ `HADOOP_HOME` vào `config/hadoop` (winutils) để Spark ghi được Parquet cục bộ.

## Hệ quả
- Muốn chạy Spark trên host Windows với HDFS trong Docker, máy host phải phân giải được tên `datanode`. Cách được hỗ trợ là chạy pipeline trong container `spark-processor` (cùng mạng Compose).
- Không có HDFS thì pipeline vẫn chạy đủ. HDFS là phần mở rộng có số đo riêng (xem `02-assessment.md`), không nằm trên đường găng của demo.
