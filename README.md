# Hệ thống Hỗ trợ Điều phối viên Rà soát Thời tiết Sân bay (Big Data Project)

Theo dõi và rà soát thời tiết 3 sân bay:
- **Đà Nẵng (`VVDN`)** (Sân bay kiểm tra trọn quy trình đầu tiên)
- **Nội Bài (`VVNB`)**
- **Tân Sơn Nhất (`VVTS`)**

---

## Kiến trúc Hệ thống (End-to-End Big Data Architecture)

Hệ thống bao gồm ba tầng xử lý dữ liệu phối hợp:

### 1. Mô-đun 1: Quan trắc Mặt đất & Dự báo Thời tiết
- **METAR (Aviation Weather Center):** Quan trắc thực tế tại sân bay mỗi 10 phút, bảo toàn bản tin gốc `rawOb`, tầm nhìn `visib`, hiện tượng thời tiết `wxString`.
- **Open-Meteo API:** Dự báo mưa và các thông số khí tượng theo từng giờ cho từng sân bay mỗi 60 phút.
- **Phong bì Dữ liệu Thô (Raw Envelope):** Lưu trữ theo chuẩn phân vùng Hive (`data/raw/<source>/date=YYYY-MM-DD/`) bằng cơ chế ghi nguyên tử (**Atomic Write** qua tệp `.tmp`) nhằm chống mất mát khi mất điện hoặc lỗi mạng. Nhật ký kiểm toán được ghi tự động vào `data/logs/ingestion_log.jsonl`.

### 2. Mô-đun 2: Ảnh Vệ tinh Địa tĩnh Himawari-9 & Lưu trữ Phân tán Hadoop HDFS
- **Himawari-9 AHI B13 (Clean IR 10.4µm):** Tự động tải các phân đoạn vệ tinh từ kho lưu trữ công cộng NOAA trên AWS S3 (`noaa-himawari9`), hỗ trợ đồng bộ theo chu kỳ 10 phút.
- **Xử lý Không gian Satpy:** Cắt lưới không gian theo tọa độ sân bay, lọc nhiễu vật lý ngoài ngưỡng `[150.0 K, 340.0 K]`, tính toán chỉ số thống kê hai hộp (Outer $\pm 1.0^\circ$, Inner $\pm 0.25^\circ$), tỷ lệ mây lạnh (< 235 K) và đối lưu mạnh (< 220 K).
- **Trực quan hóa Bản đồ Nhiệt:** Xuất ảnh PNG chú giải với tâm sân bay, viền hộp trong và chân trang nguồn bắt buộc (`JMA/NOAA Himawari-9 B13 via AWS Open Data`).
- **Cách ly & Kiểm soát Tệp Hỏng:** Kiểm tra tính toàn vẹn phần đầu giải nén BZ2/HSD (tối thiểu 1500 bytes); cô lập và ghi nhận tệp hỏng vào `data/logs/himawari_file_errors.jsonl`.
- **Đồng bộ Lưu trữ Hadoop HDFS (WebHDFS REST):** Tự động đẩy dữ liệu sang Hadoop HDFS (`/data/raw/`, `/data/processed/`), đo kiểm và ghi nhật ký thông lượng (MB/s) vào `data/logs/hdfs_sync_log.jsonl`.

### 3. Mô-đun 3: Xử lý Dữ liệu Lớn với PySpark & Động cơ Phân cấp Ưu tiên
- **Đọc Dữ liệu Thô theo Schema Chặt chẽ:** Đọc trực tiếp các tệp JSON METAR, Open-Meteo và Himawari Metrics từ thư mục cục bộ hoặc Hadoop HDFS.
- **Cách ly Lỗi (Quarantine) & Đối soát Bảo toàn Dòng (Reconciliation):** Tách biệt các bản ghi lỗi cấu trúc, thiếu khóa bắt buộc hoặc vi phạm ngưỡng vật lý vào `data/processed/quarantine/`. Đảm bảo bất biến đối soát `rows_read == rows_valid_pre_dedup + rows_quarantined` và xuất báo cáo tại `data/processed/reconciliation/reconciliation_summary.json`.
- **Chuẩn hóa Khí tượng & Phân loại Mây Vệ tinh:** Chuẩn hóa tầm nhìn (giới hạn 9999m), trích xuất mã thời tiết mưa/dông (`-RA`, `TSRA`, `SHRA`, v.v.), gắn nhãn tầng mây đối lưu B13 (`SEVERE_CONVECTIVE_CORE`, `COLD_OPAQUE_CORE`, v.v.).
- **Ghép Dữ liệu theo Điểm Thời gian (Point-in-Time Join):** Tránh rò rỉ dữ liệu tương lai (zero future data leakage) bằng cách chỉ liên kết các bản ghi có thời điểm tạo/quan trắc trước hoặc đúng thời điểm rà soát (`review_slot_utc`).
- **Động cơ Phân cấp Ưu tiên Đa yếu tố (Levels 1–4):** Xếp hạng độ nguy hiểm thời tiết theo 4 cấp bậc và xử lý đồng mức ưu tiên (`co-priority ties`) công bằng, kèm giải thích tiếng Việt tường minh.
- **Bảng Quyết định (Decision Table):** Xuất bảng dữ liệu 3 dòng/lượt rà soát (mỗi sân bay 1 dòng) dạng JSONL/Parquet phục vụ Dashboard Streamlit ở Mô-đun 4.

---

## Hướng dẫn Vận hành Nhanh (Dành cho Người mới bắt đầu)

### 1. Khởi động Cụm Hadoop HDFS (Docker Compose)
Trước khi đồng bộ dữ liệu sang kho lưu trữ phân tán HDFS, khởi động NameNode và DataNode:
```bash
docker compose up -d namenode datanode
```
Kiểm tra trạng thái giao diện web Hadoop tại: `http://localhost:9870`.

### 2. Chạy 1 Chu kỳ Tích hợp Đầy đủ (METAR + Open-Meteo + Himawari-9)
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --once --include-himawari
```

### 3. Chạy Tái hiện Lịch sử Vệ tinh theo Cửa sổ Thời gian (Himawari Window Backfill)
Backfill toàn bộ các khung thời gian 10 phút trong khoảng thời gian xác định và ghim (pin) không bị xóa bởi chính sách dọn dẹp bộ nhớ:
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --himawari-window 2026-11-09T06:00:00Z 2026-11-09T08:30:00Z
```

### 4. Đồng bộ Thư mục Dữ liệu sang Hadoop HDFS & Đo Thông lượng
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --sync-hdfs
```
Kết quả đo thông lượng và số tệp đồng bộ sẽ được ghi vào `data/logs/hdfs_sync_log.jsonl`.

### 5. Thử nghiệm Phát hiện và Cách ly Tệp Hỏng (Synthetic Corrupt Test)
Sinh tệp giả lập bị hỏng nhãn chuẩn để kiểm tra tính năng tự phòng vệ:
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --inject-corrupt-test
```
Xem nhật ký phát hiện tại `data/logs/himawari_file_errors.jsonl`.

### 6. Bật Chế độ Thu thập Ngầm Liên tục (Daemon Mode)
- **Trên Windows (1 chạm):** Click đúp chuột vào tệp `run_daemon.bat` (đã tích hợp tự động thu thập vệ tinh Himawari-9 mỗi 10 phút).
- **Hoặc chạy dòng lệnh:**
  ```bash
  .venv\Scripts\python.exe -m src.ingestion.run_ingest --loop --include-himawari
  ```
- **Hoặc chạy nền bằng Docker:**
  ```bash
  docker compose up -d ingestor
  docker compose logs -f ingestor
  ```

### 7. Chạy Pipeline PySpark & Động cơ Phân cấp Ưu tiên (Mô-đun 3)
Chạy pipeline phân tích dữ liệu, đối soát và sinh bảng quyết định:
- **Chế độ Replay lịch sử (ví dụ các mốc 14:00 và 15:00 UTC):**
  ```bash
  .venv\Scripts\python.exe -m src.spark.run_pipeline --storage-root data --mode replay --review-slots "2026-11-09T07:00:00Z,2026-11-09T08:00:00Z"
  ```
- **Chế độ Live (lấy mốc mới nhất hiện có):**
  ```bash
  .venv\Scripts\python.exe -m src.spark.run_pipeline --storage-root data --mode live
  ```
- **Kiểm thử cách ly bản ghi hỏng (Synthetic Quarantine Test):**
  ```bash
  .venv\Scripts\python.exe -m src.spark.run_pipeline --inject-quarantine-test
  ```

### 8. Chạy Toàn bộ Bộ Kiểm thử Tự động (Unit & Integration Tests)
```bash
.venv\Scripts\pytest.exe tests/ -v
```

---

## Cấu trúc Dự án
```text
DLL/
├── config/
│   ├── airports.json                 # Tọa độ, mã ICAO và phân đoạn vệ tinh của 3 sân bay
│   └── hadoop/bin/                   # winutils.exe & hadoop.dll cho PySpark trên Windows
├── src/
│   ├── ingestion/                    # Mô-đun 1 & 2: Thu thập và chuẩn bị dữ liệu
│   │   ├── config_loader.py          # Đọc và xác thực cấu hình sân bay
│   │   ├── storage.py                # Ghi tệp nguyên tử (.tmp -> .json) & ghi log audit
│   │   ├── http_client.py            # Gọi HTTP với cơ chế retry và exponential backoff
│   │   ├── metar_client.py           # Thu thập bản tin METAR
│   │   ├── open_meteo_client.py      # Thu thập dự báo Open-Meteo
│   │   ├── himawari_client.py        # Tải phân đoạn Himawari-9 S3 & quản lý vòng đời tệp thô
│   │   ├── satpy_processor.py        # Cắt tọa độ bằng Satpy, tính chỉ số đám mây, vẽ PNG
│   │   ├── hdfs_client.py            # Khách WebHDFS đẩy dữ liệu & đo benchmark thông lượng
│   │   └── run_ingest.py             # Điểm điều phối CLI chính (--once, --loop, v.v.)
│   └── spark/                        # Mô-đun 3: Xử lý Big Data & Động cơ Ưu tiên
│       ├── session.py                # Khởi tạo SparkSession tối ưu cục bộ/HDFS
│       ├── schemas.py                # Schema PySpark nghiêm ngặt cho 3 nguồn dữ liệu
│       ├── readers.py                # Trình đọc JSON hỗ trợ _corrupt_record
│       ├── quarantine.py             # Phân tách cách ly & báo cáo đối soát bảo toàn dòng
│       ├── transforms.py             # Chuẩn hóa tầm nhìn, mã thời tiết, phân loại mây B13
│       ├── temporal_join.py          # Ghép dữ liệu Point-in-Time không rò rỉ tương lai
│       ├── priority_engine.py        # Động cơ phân cấp Level 1–4 & giải quyết đồng mức
│       └── run_pipeline.py           # Điều phối CLI pipeline PySpark và xuất bảng quyết định
├── tests/
│   ├── ingestion/                    # Kiểm thử đơn vị cho tầng Ingestion (43 test cases)
│   └── spark/                        # Kiểm thử đơn vị & tích hợp cho PySpark (18 test cases)
├── data/
│   ├── raw/                          # Lớp dữ liệu thô (Hive partition YYYY-MM-DD)
│   │   ├── metar/date=YYYY-MM-DD/
│   │   ├── open_meteo/date=YYYY-MM-DD/
│   │   ├── himawari_hsd/date=YYYY-MM-DD/slot=HHMM/
│   │   └── himawari_metrics/date=YYYY-MM-DD/
│   ├── processed/
│   │   ├── himawari_png/airport=ICAO/date=YYYY-MM-DD/
│   │   ├── decision_table/           # decision_table.jsonl (3 dòng/lượt rà soát)
│   │   ├── quarantine/               # quarantine_records.jsonl
│   │   └── reconciliation/           # reconciliation_summary.json (bảo toàn dòng)
│   └── logs/                         # Nhật ký kiểm toán & chỉ số 5V
│       ├── ingestion_log.jsonl       # Bảng đối soát số lượt quét, số bản ghi, byte thực tải
│       ├── himawari_file_errors.jsonl# Nhật ký cô lập tệp nhị phân vệ tinh bị hỏng
│       └── hdfs_sync_log.jsonl       # Nhật ký đo kiểm thông lượng WebHDFS (MB/s)
├── docker-compose.yml                # Docker Compose bao gồm Ingestor, NameNode, DataNode, Spark
├── Dockerfile                        # Đóng gói container chạy ngầm
├── requirements.txt                  # Thư viện phụ thuộc (PySpark, Satpy, Requests, v.v.)
└── run_daemon.bat                    # Kịch bản khởi chạy 1 chạm có Himawari-9 trên Windows
```
