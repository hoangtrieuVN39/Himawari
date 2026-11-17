# Hệ thống Hỗ trợ Điều phối viên Rà soát Thời tiết Sân bay (Big Data Project)

Theo dõi và rà soát thời tiết 3 sân bay:
- **Đà Nẵng (`VVDN`)** (Sân bay kiểm tra trọn quy trình đầu tiên)
- **Nội Bài (`VVNB`)**
- **Tân Sơn Nhất (`VVTS`)**

---

## Bắt đầu nhanh

Yêu cầu: Windows, Python 3.11, Java 17 (cho PySpark). Docker chỉ cần nếu dùng HDFS hoặc chạy bằng container.

```bash
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m src.spark.run_pipeline --storage-root data   # dựng lại Gold từ dữ liệu mẫu, khoảng 1 phút
run_dashboard.bat                                                        # mở http://localhost:8501
```

## Tài liệu

| Cần biết | Mở |
|---|---|
| Toàn bộ hệ thống chạy thế nào | `docs/handoff/he-thong-hien-tai.md` |
| Kết quả đã chạy, rủi ro, việc còn lại | `docs/handoff/handoff-nhom.md` và `docs/handoff/ket-qua/` |
| Checklist việc của từng người | `docs/handoff/checklist-cong-viec.xlsx` |
| Hiện trạng, đánh giá, kiến trúc đích, roadmap, ADR, từ điển dữ liệu | `docs/architecture/` |
| Kế hoạch dự án nhóm và thang điểm | `docs/ke-hoach-nhom.md` |

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
- **Đọc Dữ liệu Thô theo Schema Chặt chẽ:** Đọc các shard JSONL (Bronze) của METAR, Open-Meteo và Himawari Metrics từ layout đang active trên đĩa cục bộ (`--storage-root data`) hoặc từ HDFS (`--storage-root hdfs://namenode:9000/data`).
- **Cách ly Lỗi (Quarantine) & Đối soát Bảo toàn Dòng (Reconciliation):** Tách biệt các bản ghi lỗi cấu trúc, thiếu khóa bắt buộc hoặc vi phạm ngưỡng vật lý vào bảng Silver `quarantine`. Đảm bảo bất biến đối soát `rows_read == rows_valid_pre_dedup + rows_quarantined` và ghi bảng Gold `reconciliation`.
- **Chuẩn hóa Khí tượng & Phân loại Mây Vệ tinh:** Chuẩn hóa tầm nhìn (giới hạn 9999m), trích xuất mã thời tiết mưa/dông (`-RA`, `TSRA`, `SHRA`, v.v.), gắn nhãn tầng mây đối lưu B13 (`SEVERE_CONVECTIVE_CORE`, `COLD_OPAQUE_CORE`, v.v.).
- **Ghép Dữ liệu theo Điểm Thời gian (Point-in-Time Join):** Tránh rò rỉ dữ liệu tương lai (zero future data leakage) bằng cách chỉ liên kết các bản ghi có thời điểm tạo/quan trắc trước hoặc đúng thời điểm rà soát (`review_slot_utc`).
- **Động cơ Phân cấp Ưu tiên Đa yếu tố (Levels 1–4):** Xếp hạng độ nguy hiểm thời tiết theo 4 cấp bậc và xử lý đồng mức ưu tiên (`co-priority ties`) công bằng, kèm giải thích tiếng Việt tường minh.
- **Bảng Quyết định (Decision Table):** Ghi bảng Gold `decision_table` 3 dòng/lượt rà soát (mỗi sân bay 1 dòng) dạng Parquet, kèm `slot_kind`, `inputs_complete_at_utc`, `first_published_at_utc`.
- **Lake Parquet có phiên bản (Medallion):** Bronze (`data/raw_compact`) → Silver (`data/lake/silver/…`) → Gold (`data/lake/gold/…`). Mỗi lần chạy commit một phiên bản bất biến `run_id=…` (hiển thị khi có `_SUCCESS`, giữ 2 phiên bản mới nhất), ghi được cả lên đĩa cục bộ lẫn `hdfs://` (`--lake-root`). Chi tiết: [`docs/architecture/`](docs/architecture/).
- **Runner Spark chạy lâu dài:** `run_pipeline --loop` giữ một SparkSession và công bố lại Gold mỗi khi Bronze thay đổi (khoảng 18 s mỗi chu kỳ ấm).

### 4. Mô-đun 4: Dashboard Streamlit Rà soát Quyết định, Phát lại Lịch sử (Replay), Đo lường KPI & Đối soát Chất lượng Dữ liệu
- **Giao diện Đa Tab Trực quan:** 4 Tab: Quyết định, Đo KPI, Đối soát dữ liệu, Sức khỏe hệ thống. Dashboard **chỉ đọc** lake Gold; Spark là nguồn sự thật duy nhất cho mức ưu tiên. Nạp dữ liệu tức thì (< 1s) ở chế độ ngoại tuyến (Offline Ready).
- **Bảng Rà soát Quyết định & Xử lý Đồng mức (Tab 1):** Hiển thị song song giờ UTC (`HH:MMZ`) và Giờ Việt Nam (`ICT`), nêu bật sân bay ưu tiên và banner cảnh báo màu cam khi có đồng mức ưu tiên đầu (`Co-Priority Tie`), hỗ trợ đảo thứ tự dòng hiển thị mà không ảnh hưởng kết quả.
- **Truy nguyên Bằng chứng 3 Nguồn (Provenance Drill-Down):** Bản tin METAR gốc (giờ nhận, tuổi dữ liệu, hiện tượng thời tiết), mã dự báo mưa Open-Meteo, và chuỗi tối đa 6 ảnh vệ tinh Himawari-9 B13 trước sự kiện (Time-Lapse 10 phút/ảnh) kèm bộ chỉ số Dual-Box ($\pm 1.0^\circ$ và $\pm 0.25^\circ$).
- **Đo lường & Chấm điểm Tự động KPI (Tab 2 — CLO4):** Tích hợp bộ bấm giờ nhận ca, đối chiếu giữa chế độ Xem 3 Nguồn Rời (Baseline) và Xem Dashboard Tổng hợp, tự động chấm điểm và tính toán % rút ngắn thời gian trung vị đạt mục tiêu $\ge 30\%$.
- **Đối soát Bảo toàn Dòng & Quarantine (Tab 3 — CLO2/CLO3):** Bảng đối soát bảo toàn dòng cho cả 3 nguồn, trình xem bản ghi cách ly lỗi, nhật ký lỗi tệp nhị phân và thống kê thông lượng Hadoop HDFS.
- **Sức khỏe hệ thống (Tab 4):** SLA độ mới từng nguồn, độ phủ lượt quét, nguyên nhân khoảng thiếu (nguồn thiếu / lỗi máy thu), bất thường (MAD), trôi dữ liệu (PSI), độ trễ công bố P50/P95 và thời gian từng giai đoạn Spark.

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

**Tải bù dài ngày (big-data backfill):**
```bash
# METAR tối đa 30 ngày: AWC giữ 30 ngày, 400 bản ghi/request; gọi theo từng sân bay × 24 h, throttle 0,7 s
.venv\Scripts\python.exe -m src.ingestion.run_ingest --backfill-days 30

# Himawari phân tán bằng Spark (luôn tải đủ S0410 + S0510). Nên chạy ở ổ đĩa còn nhiều chỗ trống,
# rồi nhập phần metrics (nhỏ) vào Bronze của dự án
.venv\Scripts\python.exe -m src.spark.himawari_batch --start 2026-11-09T00:00:00Z --end 2026-11-15T23:50:00Z --parallelism 2 --base-dir D:/DLL_backfill/data
.venv\Scripts\python.exe -m src.ingestion.import_metrics --from D:/DLL_backfill/data --to data
```
Đo B8: khoảng 19 s/lượt quét khi chạy tuần tự, 5,67 MB tải về mỗi lượt quét. Chạy p=4 nhanh hơn 2,44×, nên 7 ngày mất khoảng 2,5 h.
Open-Meteo không có tải bù: ca lịch sử thiếu dự báo thì hiển thị "dự báo khuyết".

### 4. Đồng bộ Thư mục Dữ liệu sang Hadoop HDFS & Đo Thông lượng
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --sync-hdfs
```
Cơ chế HDFS Sync tự động nhận biết layout hoạt động (`daily_jsonl` hoặc `legacy_files`), chụp snapshot dưới khóa tiến trình, so khớp mã băm SHA-256 qua manifest `data/logs/hdfs_sync_manifest.json` để bỏ qua các tệp không thay đổi (`files_skipped_unchanged`), và chỉ chuyển đổi remote marker `storage_state.json` sau khi hoàn tất 100% upload dữ liệu. Kết quả đo thông lượng và số tệp đồng bộ sẽ được ghi vào `data/logs/hdfs_sync_log.jsonl`.

### 5. Quản lý và Chuyển đổi Lưu trữ Thu gọn (Compact Raw Storage Migration)
Chuyển đổi các tệp JSON đơn lẻ rời rạc sang tệp gộp theo ngày `batches-0001.jsonl` (tự chia shard khi vượt ngưỡng 64 MiB), đối chiếu đa tập hợp mã băm Canonical SHA-256 và giải phóng inode đĩa:
```bash
# Bước 1: Kiểm tra tính toàn vẹn và dự báo số lượng tệp gom (chỉ đọc, không sửa đổi đĩa)
.venv\Scripts\python.exe -m src.ingestion.migrate_storage --base-dir data --dry-run

# Bước 2: Thực hiện chuyển đổi và kích hoạt layout mới (daily_jsonl)
.venv\Scripts\python.exe -m src.ingestion.migrate_storage --base-dir data --apply

# Bước 3: Đóng gói toàn bộ JSON cũ vào raw-json-before-migration.zip, xác thực SHA-256 rồi dọn tệp cũ
.venv\Scripts\python.exe -m src.ingestion.migrate_storage --base-dir data --prune-legacy

# (Tùy chọn) Khôi phục lại trạng thái tệp rời nếu cần thiết (tự bảo tồn các batch mới sinh sau cutover)
.venv\Scripts\python.exe -m src.ingestion.migrate_storage --base-dir data --rollback

# (Tùy chọn) Kiểm tra mọi batch đã migrate vẫn còn trong raw_compact
.venv\Scripts\python.exe -m src.ingestion.migrate_storage --base-dir data --verify
```
Chạy lại `--apply` sau khi đã kích hoạt là no-op (`ALREADY_ACTIVE`); `--prune-legacy` chỉ xóa tệp chưa bị sửa kể từ lúc migrate. Báo cáo lưu ở `data/logs/migration_report_<timestamp>.json`.

### 6. Thử nghiệm Phát hiện và Cách ly Tệp Hỏng (Synthetic Corrupt Test)
Sinh tệp giả lập bị hỏng nhãn chuẩn để kiểm tra tính năng tự phòng vệ:
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --inject-corrupt-test
```
Xem nhật ký phát hiện tại `data/logs/himawari_file_errors.jsonl`.

### 7. Bật Chế độ Thu thập Ngầm Liên tục (Daemon Mode)
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

### 8. Chạy Pipeline PySpark & Động cơ Phân cấp Ưu tiên (Mô-đun 3)
Chạy pipeline phân tích dữ liệu, đối soát và commit lake Silver/Gold (`data/lake/`):
- **Mặc định (`--review-slots all`):** các mốc replay trong `config/replay_slots.json` cộng mọi mốc live (giờ lấy METAR không phải tải bù, làm tròn lên 5 phút):
  ```bash
  .venv\Scripts\python.exe -m src.spark.run_pipeline --storage-root data
  ```
- **Chỉ một số mốc (replay):**
  ```bash
  .venv\Scripts\python.exe -m src.spark.run_pipeline --storage-root data --mode replay --review-slots "2026-11-09T07:00:00Z,2026-11-09T08:00:00Z"
  ```
- **`--review-slots auto`:** các mốc replay cộng mốc live mới nhất. `--mode live` yêu cầu METAR/B13 phải được lấy về trước mốc. Dự báo luôn phải được lấy về trước mốc ở mọi chế độ.
- **Runner chạy lâu dài (khuyến nghị khi demo LIVE):**
  ```bash
  .venv\Scripts\python.exe -m src.spark.run_pipeline --storage-root data --loop --poll-seconds 15
  docker compose --profile spark up -d spark-runner   # hoặc chạy trong Docker
  ```
- **Ghi lake lên HDFS** (chạy trong container để phân giải được `datanode`):
  ```bash
  docker compose --profile spark run --rm spark-processor python -m src.spark.run_pipeline --storage-root hdfs://namenode:9000/data --lake-root hdfs://namenode:9000/lake
  ```
- **Hiệu năng (đo 2026-11-16, 134 mốc):** lần chạy lạnh khoảng 52 s, lần chạy ấm khoảng 18 s. Thời gian từng giai đoạn được ghi vào `data/logs/pipeline_runs.jsonl`. Trước đây mỗi lần chạy mất khoảng 3,5 phút; nguyên nhân là spine dựng bằng Python RDD (xem ADR-004).
- **Kiểm thử cách ly bản ghi hỏng (Synthetic Quarantine Test):**
  ```bash
  .venv\Scripts\python.exe -m src.spark.run_pipeline --inject-quarantine-test
  ```

### 9. Khởi chạy Giao diện Dashboard Streamlit (Mô-đun 4)
- **Cách 1: Khởi chạy 1 chạm trên Windows:**
  Click đúp vào file `run_dashboard.bat` trong thư mục gốc dự án.
- **Cách 2: Khởi chạy bằng dòng lệnh (.venv):**
  ```bash
  .venv\Scripts\streamlit.exe run src/dashboard/app.py
  ```
- **Cách 3: Khởi chạy bằng Docker Compose:**
  ```bash
  docker compose up -d dashboard
  ```
  Truy cập giao diện tại: `http://localhost:8501`.

### 10. Chạy Toàn bộ Bộ Kiểm thử Tự động (Unit & Integration Tests)
```bash
.venv\Scripts\pytest.exe -q
```
Bộ kiểm thử có 333 test, chạy khoảng 10–16 phút vì phần Spark khởi động chậm trên Windows. Test dashboard và test thu nhận chạy nhanh (dưới 1 phút); để chạy riêng: `pytest tests/dashboard tests/ingestion -q`. Test Spark đọc dữ liệu mẫu đã commit trong `data/raw_compact/`.

### 11. Đóng gói Bộ Mẫu Nộp Bài
```bash
.venv\Scripts\python.exe scripts/package_sample.py --storage-root data --out dist/sample_dataset.zip
```
Gói gồm cửa sổ phát lại đã ghim, bảng Gold và nhật ký kiểm toán; không chứa dữ liệu tải bù hay khóa truy cập. `dist/` không được commit.

---

## Cấu trúc Dự án
```text
DLL/
├── config/
│   ├── airports.json                 # Tọa độ, mã ICAO và phân đoạn vệ tinh của 3 sân bay
│   ├── replay_slots.json             # 3 mốc phát lại cố định cho demo và đo KPI
│   └── hadoop/                       # hadoop.env cho Docker; bin/ có winutils.exe, hadoop.dll cho PySpark trên Windows
├── src/
│   ├── ingestion/                    # Thu thập METAR, Open-Meteo, Himawari-9; ghi Bronze nguyên tử; tải bù; HDFS
│   ├── spark/                        # Pipeline PySpark: schema, quarantine, ghép đúng thời điểm, mức ưu tiên, ghi lake, runner
│   ├── dashboard/                    # Streamlit (4 tab, chỉ đọc Gold), KPI evaluator, giải mã METAR
│   ├── diagnostics/                  # Chẩn đoán vận hành: độ mới, độ phủ, nguyên nhân thiếu, bất thường, trôi dữ liệu
│   ├── analytics/                    # Kiểm định dự báo mưa và thứ tự Mức 1–4
│   └── ml/                           # Nhãn, đặc trưng, huấn luyện, suy luận dự đoán 0–60′ (chỉ để tham khảo)
├── tests/                            # 333 test, chia theo mô-đun như src/
├── scripts/
│   └── package_sample.py             # Đóng gói bộ mẫu nộp bài
├── data/
│   ├── raw_compact/                  # Bronze: JSONL theo nguồn và ngày (date=YYYY-MM-DD)
│   ├── lake/                         # Silver (quarantine) và Gold (decision_table, reconciliation, health, ...) dạng Parquet có phiên bản
│   ├── processed/                    # Ảnh PNG B13 và bảng quyết định JSONL dự phòng ngoại tuyến
│   └── logs/                         # Nhật ký thu nhận, lỗi tệp, HDFS, pipeline, sức khỏe, KPI
├── models/                           # Mô hình dự đoán đã huấn luyện (LATEST trỏ tới bản mới nhất)
├── docs/
│   ├── ke-hoach-nhom.md              # Kế hoạch dự án nhóm và thang điểm
│   ├── architecture/                 # Hiện trạng, đánh giá, kiến trúc đích, roadmap, từ điển dữ liệu, ADR
│   ├── handoff/                      # Tài liệu bàn giao, checklist Excel, bảng kết quả
│   └── superpowers/plans/            # Kế hoạch triển khai từng mô-đun
├── docker-compose.yml                # Ingestor, dashboard, NameNode, DataNode, Spark (profile `spark`)
├── Dockerfile                        # Image chạy ngầm
├── requirements.txt                  # Thư viện phụ thuộc
├── pytest.ini
├── run_daemon.bat                    # Khởi chạy 1 chạm thu thập dữ liệu (có Himawari-9) trên Windows
└── run_dashboard.bat                 # Khởi chạy 1 chạm Dashboard trên Windows
```

Không được commit: `.venv/`, `dist/`, `.harness/`, tệp thô vệ tinh `.DAT.bz2` (trừ một tệp mẫu tổng hợp), các bảng Silver nguồn (dựng lại bằng pipeline), tệp checksum `*.crc`, khóa truy cập.
