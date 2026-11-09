# Hệ thống Hỗ trợ Điều phối viên Rà soát Thời tiết Sân bay (Big Data Project)

Theo dõi và rà soát thời tiết 3 sân bay:
- **Đà Nẵng (`VVDN`)** (Sân bay kiểm tra trọn quy trình đầu tiên)
- **Nội Bài (`VVNB`)**
- **Tân Sơn Nhất (`VVTS`)**

---

## Mô-đun 1: Tầng Thu thập Dữ liệu (Data Ingestion Layer)

Mô-đun 1 thu thập tự động và bảo toàn dữ liệu gốc từ 2 nguồn API:
1. **METAR (Aviation Weather Center):** Quan trắc thực tế tại sân bay mỗi 10 phút, lưu trữ đầy đủ bản tin gốc `rawOb`, tầm nhìn `visib`, hiện tượng thời tiết `wxString`.
2. **Open-Meteo API:** Dự báo mưa và thông số khí tượng theo giờ cho từng sân bay mỗi 60 phút.

Dữ liệu được lưu dưới dạng **Phong bì JSON (Raw Envelope)** trong thư mục phân vùng Hive (`data/raw/<source>/date=YYYY-MM-DD/`) bằng cơ chế **Atomic Write** (tránh hỏng tệp khi mất điện), đồng thời ghi nhật ký truy cập vào `data/logs/ingestion_log.jsonl` để đối soát chỉ số 5V.

---

## Hướng dẫn Vận hành Nhanh (Dành cho Người mới bắt đầu)

### 1. Chạy lấy mẫu dữ liệu 24 giờ gần nhất (Chạy 1 lần)
```bash
.venv\Scripts\python.exe -m src.ingestion.run_ingest --once --backfill-hours 24
```

### 2. Bật thu thập ngầm liên tục 7 ngày (Canh chu kỳ đồng hồ UTC)
- **Cách 1 (Windows nhanh nhất):** Double click vào tệp `run_daemon.bat` hoặc gõ:
  ```bash
  .venv\Scripts\python.exe -m src.ingestion.run_ingest --loop
  ```
- **Cách 2 (Bằng Docker Compose):**
  ```bash
  docker compose up -d ingestor
  docker compose logs -f ingestor
  ```

### 3. Chạy toàn bộ Unit Tests (TDD)
```bash
.venv\Scripts\pytest.exe tests/ -v
```

---

## Cấu trúc Dự án
```text
DLL/
├── config/
│   └── airports.json                 # Tọa độ và mã ICAO của 3 sân bay
├── src/
│   └── ingestion/
│       ├── config_loader.py          # Đọc và xác thực cấu hình sân bay
│       ├── storage.py                # Ghi tệp nguyên tử (.tmp -> .json) & ghi log audit
│       ├── http_client.py            # Gọi HTTP với cơ chế retry và exponential backoff
│       ├── metar_client.py           # Thu thập bản tin METAR
│       ├── open_meteo_client.py      # Thu thập dự báo Open-Meteo
│       └── run_ingest.py             # Điểm chạy CLI chính (--once, --loop)
├── tests/ingestion/                  # Toàn bộ 14 bài unit test bao phủ các ca lỗi và thành công
├── data/
│   ├── raw/                          # Lớp dữ liệu thô (Raw Layer)
│   │   ├── metar/date=YYYY-MM-DD/
│   │   └── open_meteo/date=YYYY-MM-DD/
│   └── logs/
│       └── ingestion_log.jsonl       # Bảng đối soát số lượt quét, số bản ghi, byte thực tải
├── docs/superpowers/
│   ├── specs/                        # Tài liệu thiết kế kiến trúc
│   └── plans/                        # Kế hoạch triển khai chi tiết
├── requirements.txt                  # Thư viện phụ thuộc
├── Dockerfile & docker-compose.yml   # Đóng gói container chạy ngầm
└── run_daemon.bat                    # Script chạy 1 chạm cho Windows
```
