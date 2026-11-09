# Tài liệu Thiết kế (Spec) — Mô-đun 1: Thu thập Dữ liệu Khí tượng (METAR & Open-Meteo) với Docker Compose

**Ngày tạo:** 2026-11-09  
**Dự án:** Hỗ trợ điều phối viên rà soát thời tiết tại ba sân bay Việt Nam (`VVDN`, `VVNB`, `VVTS`)  
**Phạm vi:** Mô-đun 1 (Mốc Tuần 1 · Buổi 1 — Thiết lập nền tảng Docker Compose, thu thập bản gốc METAR và dự báo Open-Meteo, ghi nhật ký truy cập để đối soát 5V và độ phủ).

---

## 1. Mục tiêu và Bối cảnh Nghiệp vụ

Mô-đun 1 đóng vai trò là tầng **Thu nhận dữ liệu (Data Ingestion / Landing Layer)** cho toàn bộ hệ thống Big Data của nhóm:
1. **Bật lưu dữ liệu từ ngày đầu tiên:** Thu thập liên tục bản tin quan trắc tại trạm (`METAR` từ Aviation Weather Center) mỗi 10 phút và dự báo lượng mưa theo giờ (`Open-Meteo`) mỗi 60 phút cho 3 sân bay: Đà Nẵng (`VVDN`), Nội Bài (`VVNB`), và Tân Sơn Nhất (`VVTS`).
2. **Bảo toàn phiên bản dự báo theo thời gian thực (`fetched_at_utc`):** Mỗi lần gọi API dự báo được lưu thành một tệp riêng biệt kèm nhãn thời gian hệ thống nhận dữ liệu. Điều này đảm bảo khi chạy PySpark (Mô-đun 3) và phát lại ca (Mô-đun 4), hệ thống chỉ sử dụng đúng phiên bản dự báo đã có trước thời điểm rà soát (chống rò rỉ dữ liệu tương lai).
3. **Đóng gói bằng Docker Compose:** Đảm bảo cả 3 thành viên trong nhóm chạy cùng một môi trường Python chuẩn xác trên Windows/macOS/Linux mà không gặp xung đột thư viện, đồng thời hỗ trợ chạy ngầm 24/7 (`restart: unless-stopped`) và chạy 1 lần lấy mẫu ngay (`--once --backfill-hours 24`).
4. **Minh chứng cho Buổi 1 & Buổi 3:** Tạo ra mẫu dữ liệu thật từ 2 nguồn đầu tiên, phiên bản dự báo đầu tiên, từ điển dữ liệu lớp Raw và nhật ký truy cập (`ingestion_log.jsonl`) ghi nhận số tệp, dung lượng (byte) và trạng thái mỗi lượt quét.

---

## 2. Kiến trúc và Cấu trúc Thư mục

Hệ thống sử dụng Docker Compose với cơ chế **Bind Mount** thư mục `./data`, `./config`, và `./src` ra máy chủ (Host) để người dùng trên Windows có thể mở xem trực tiếp các tệp JSON và tệp log bằng File Explorer hoặc VS Code.

```text
DLL/
├── docker-compose.yml                # Định nghĩa dịch vụ ingestor (chạy ngầm) và runner (chạy lệnh lẻ/test)
├── Dockerfile                        # Image Python 3.11-slim cài sẵn thư viện thu thập & kiểm thử
├── requirements.txt                  # Danh sách thư viện Python cố định phiên bản
├── config/
│   └── airports.json                 # Danh mục 3 sân bay (ICAO, tên, vĩ độ, kinh độ)
├── src/
│   ├── __init__.py
│   └── ingestion/
│       ├── __init__.py
│       ├── config_loader.py          # Đọc và kiểm tra tính hợp lệ của config/airports.json
│       ├── storage.py                # Ghi tệp nguyên tử (atomic write .tmp -> .json) & ghi ingestion_log.jsonl
│       ├── http_client.py            # Gọi HTTP với cơ chế Retry + Exponential Backoff (2s, 4s, 8s)
│       ├── metar_client.py           # Thu thập bản tin METAR từ Aviation Weather Center API
│       ├── open_meteo_client.py      # Thu thập dự báo giờ từ Open-Meteo API
│       └── run_ingest.py             # CLI điều phối (--once, --backfill-hours, --loop)
├── tests/
│   └── ingestion/
│       ├── test_storage.py           # Kiểm thử atomic write, cấu trúc đường dẫn phân vùng và ghi log
│       ├── test_metar_client.py      # Kiểm thử gọi METAR API (mock dữ liệu hợp lệ và lỗi mạng)
│       └── test_open_meteo_client.py # Kiểm thử gọi Open-Meteo API và đóng gói metadata
└── data/
    ├── raw/
    │   ├── metar/
    │   │   └── date=YYYY-MM-DD/
    │   │       └── metar_<batch_id>_<YYYYMMDDTHHMMSSZ>.json
    │   └── open_meteo/
    │       └── date=YYYY-MM-DD/
    │           └── open_meteo_<ICAO>_<batch_id>_<YYYYMMDDTHHMMSSZ>.json
    └── logs/
        └── ingestion_log.jsonl       # Nhật ký truy cập & đối soát độ phủ (JSON Lines)
```

---

## 3. Cấu hình Sân bay và Nguồn Dữ liệu

### 3.1. Danh mục Sân bay (`config/airports.json`)

| Mã ICAO | Mã IATA | Tên sân bay | Vĩ độ (`latitude`) | Kinh độ (`longitude`) | Vai trò trong dự án |
|---|---|---|---:|---:|---|
| `VVDN` | `DAD` | Quốc tế Đà Nẵng | `16.0439` | `108.1994` | Sân bay kiểm tra trọn quy trình đầu tiên |
| `VVNB` | `HAN` | Quốc tế Nội Bài | `21.2212` | `105.8072` | Sân bay theo dõi chính |
| `VVTS` | `SGN` | Quốc tế Tân Sơn Nhất | `10.8188` | `106.6520` | Sân bay theo dõi chính |

### 3.2. Nguồn 1: METAR — Aviation Weather Center (NWS/NOAA)
- **Endpoint:** `https://aviationweather.gov/api/data/metar`
- **Tham số truy vấn:**
  - Chế độ thường xuyên (mỗi 10 phút): `?ids=VVDN,VVNB,VVTS&format=json&hours=2` (lấy cửa sổ 2 giờ gần nhất để không bỏ sót bản tin Sửa `COR`/`SPECI` phát sinh giữa 2 chu kỳ; khử trùng lặp sẽ do PySpark thực hiện ở Mô-đun 3).
  - Chế độ lấy mẫu ban đầu (`--backfill-hours 24`): `?ids=VVDN,VVNB,VVTS&format=json&hours=24`.
- **Headers bắt buộc:** `User-Agent: VN-Airport-Weather-Dispatcher-Academic-Project/1.0 (MIS5007)` theo đúng quy định của NWS API để không bị chặn (`HTTP 403/429`).
- **Điều khoản sử dụng:** Dữ liệu công khai của U.S. National Weather Service / Aviation Weather Center, ghi rõ nguồn phát hành, URL và thời điểm truy cập trong metadata.

### 3.3. Nguồn 2: Dự báo mưa — Open-Meteo API
- **Endpoint:** `https://api.open-meteo.com/v1/forecast`
- **Tham số truy vấn (gọi cho từng sân bay mỗi 60 phút):**
  - `latitude=<lat>&longitude=<lon>`
  - `hourly=precipitation,rain,showers,weather_code,visibility,wind_speed_10m,wind_gusts_10m`
  - `timezone=UTC`
  - `forecast_days=2` (đủ bao phủ các giờ tới và phục vụ kiểm thử phiên bản dự báo).
- **Điều khoản sử dụng:** Gói miễn phí phi thương mại (Non-commercial academic use), ghi công `"Weather data by Open-Meteo.com"` trong metadata và giao diện.

---

## 4. Định dạng Lưu trữ Bản gốc (Raw Envelope) và Nhật ký Đối soát

### 4.1. Cấu trúc phong bì JSON (Raw Envelope)
Mọi tệp lưu trong `data/raw/` đều giữ **nguyên vẹn 100% phản hồi gốc từ API** bên trong trường `payload`, đồng thời bổ sung phần đầu `metadata` để PySpark ở Mô-đun 3 đọc theo schema chuẩn:

```json
{
  "metadata": {
    "batch_id": "20261109T082000Z_metar_a1b2c3",
    "source": "aviationweather_metar",
    "source_url": "https://aviationweather.gov/api/data/metar?ids=VVDN%2CVVNB%2CVVTS&format=json&hours=2",
    "attribution": "NOAA/NWS Aviation Weather Center (https://aviationweather.gov/data/api/)",
    "expected_slot_utc": "2026-11-09T08:20:00Z",
    "fetched_at_utc": "2026-11-09T08:20:03Z",
    "http_status": 200,
    "airports_requested": ["VVDN", "VVNB", "VVTS"],
    "is_backfill": false,
    "record_count": 6
  },
  "payload": [
    {
      "icaoId": "VVDN",
      "receiptTime": "2026-11-09 08:02:15",
      "obsTime": 1794211200,
      "reportTime": "2026-11-09 08:00:00",
      "visib": "6+",
      "wxString": "-RA",
      "rawOb": "METAR VVDN 090800Z 05008KT 8000 -RA FEW015 BKN040 26/24 Q1012 NOSIG"
    }
  ]
}
```

### 4.2. Cơ chế Ghi tệp Nguyên tử (Atomic Write)
Để tránh tạo ra tệp JSON bị cắt cụt khi mất điện hoặc dừng container đột ngột:
1. Tạo thư mục đích `data/raw/<source>/date=<YYYY-MM-DD>/` nếu chưa tồn tại.
2. Ghi toàn bộ chuỗi JSON (UTF-8) vào tệp tạm `<filename>.json.tmp` và gọi `os.fsync()`.
3. Dùng `os.replace(<filename>.json.tmp, <filename>.json)` để đổi tên nguyên tử sang tệp chính thức.

### 4.3. Nhật ký Truy cập & Đối soát (`data/logs/ingestion_log.jsonl`)
Mỗi lần gọi API (dù thành công hay thất bại sau khi đã retry) đều ghi nối thêm (append) 1 dòng JSON vào `data/logs/ingestion_log.jsonl`:

```json
{
  "batch_id": "20261109T082000Z_metar_a1b2c3",
  "source": "metar",
  "airport": "ALL",
  "expected_slot_utc": "2026-11-09T08:20:00Z",
  "fetched_at_utc": "2026-11-09T08:20:03Z",
  "status": "SUCCESS",
  "http_status": 200,
  "attempt_count": 1,
  "record_count": 6,
  "bytes_written": 4128,
  "file_path": "data/raw/metar/date=2026-11-09/metar_20261109T082000Z_a1b2c3.json",
  "is_backfill": false,
  "error_type": null,
  "error_message": null
}
```

Trường hợp mạng đứt hoặc API sập (`status: "FAILED"`), dòng log vẫn được ghi với `bytes_written: 0`, `file_path: null` và chi tiết `error_type` / `error_message` để nhóm phân biệt rõ **sự cố máy thu / mất mạng** với **thiếu dữ liệu trên nguồn** khi báo cáo ở Buổi 3 và Buổi 6.

---

## 5. Chế độ Vận hành CLI và Lập lịch trong Docker

Script `src/ingestion/run_ingest.py` cung cấp các cờ điều khiển rõ ràng cho người mới bắt đầu:
1. **Chạy 1 lần và lấy mẫu 24 giờ gần nhất (dùng ngay sau khi cài đặt để có dữ liệu test):**
   ```bash
   docker compose run --rm ingestor python -m src.ingestion.run_ingest --once --backfill-hours 24
   ```
2. **Chạy ngầm liên tục 24/7 (tự động căn theo mốc 10 phút của đồng hồ UTC: `:00, :10, :20, :30, :40, :50` cho METAR và đầu mỗi giờ `:00` cho Open-Meteo):**
   ```bash
   docker compose up -d ingestor
   ```
3. **Xem log trực tiếp bất kỳ lúc nào:**
   ```bash
   docker compose logs -f ingestor
   ```

---

## 6. Chiến lược Kiểm thử và Nghiệm thu Mô-đun 1

Trước khi hoàn tất Mô-đun 1, chúng ta sẽ kiểm chứng qua 3 bước:
1. **Unit Tests (`pytest`):**
   - Kiểm tra tạo thư mục phân vùng `date=YYYY-MM-DD`, ghi atomic `.tmp -> .json`, và ghi `ingestion_log.jsonl`.
   - Kiểm tra `metar_client` và `open_meteo_client` xử lý đúng phản hồi JSON hợp lệ và tự động retry khi gặp lỗi `HTTP 503` hoặc `Timeout`.
2. **Chạy thực tế chế độ `--once --backfill-hours 24` trong Docker:**
   - Xác nhận tệp JSON thực tế của 3 sân bay (`VVDN`, `VVNB`, `VVTS`) xuất hiện trong `data/raw/metar/` và `data/raw/open_meteo/`.
   - Xác nhận `data/logs/ingestion_log.jsonl` ghi nhận `status: "SUCCESS"`, số bản ghi và dung lượng byte thực tế.
3. **Khởi chạy dịch vụ ngầm `docker compose up -d ingestor`:**
   - Kiểm tra container ở trạng thái `Up` để bắt đầu tích lũy dữ liệu cho nhóm.

