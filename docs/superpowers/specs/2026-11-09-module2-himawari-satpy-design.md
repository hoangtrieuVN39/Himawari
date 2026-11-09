# Tài liệu Thiết kế (Spec) — Mô-đun 2: Tích hợp Hadoop HDFS & Xử lý Ảnh Vệ tinh Himawari-9 Kênh B13 bằng Satpy

**Ngày tạo:** 2026-11-09  
**Dự án:** Hỗ trợ điều phối viên rà soát thời tiết tại ba sân bay Việt Nam (`VVDN`, `VVNB`, `VVTS`)  
**Phạm vi:** Mô-đun 2 (Mốc Tuần 1 · Buổi 3 & Chuẩn bị Ma trận Lưu trữ Buổi 6 — Bổ sung cụm đơn nốt Hadoop HDFS trong Docker Compose, tải phân đoạn Himawari-9 kênh B13 từ AWS S3, đọc/định vị/cắt vùng 2 vòng bằng `Satpy`, trích xuất chỉ số mây cho PySpark, xuất ảnh PNG có chú giải và ghi nhật ký lỗi tệp nhị phân riêng).

---

## 1. Mục tiêu và Quyết định Thiết kế Đã Chốt (`/grill-me`)

Qua quá trình phỏng vấn thiết kế, nhóm thống nhất 5 quyết định cốt lõi cho Mô-đun 2:

1. **Kiến trúc Lưu trữ Dual-Mode (Kho tệp cục bộ + Hadoop HDFS):**
   - Bổ sung dịch vụ `namenode` (cổng Web UI `9870`, RPC `9000`) và `datanode` (cổng `9864`) dùng image chính thức `apache/hadoop:3` vào `docker-compose.yml`.
   - Hỗ trợ **Auto Dual-Write** (ghi cục bộ vào `data/` đồng thời đẩy lên HDFS qua WebHDFS REST API nếu cụm HDFS đang bật) và công cụ đồng bộ bù **`sync_to_hdfs`** (đẩy toàn bộ dữ liệu cục bộ lên HDFS kèm đo tốc độ/độ trễ ghi để làm bằng chứng định lượng cho Ma trận lưu trữ ở Buổi 6).
2. **Tối ưu Tải Phân đoạn Himawari-9 HSD từ AWS S3 (`noaa-himawari9`):**
   - Mỗi chu kỳ quét 10 phút của vệ tinh Himawari-9 (`AHI-L1b-FLDK`), chỉ tải **1 kênh (`B13` - hồng ngoại bước sóng $10.4\,\mu\text{m}$)** và **2 phân đoạn**:
     - Phân đoạn **`S0410`** (vĩ độ $\sim 12^\circ\text{N} - 26^\circ\text{N}$): Phủ cả **Đà Nẵng (`VVDN`)** và **Nội Bài (`VVNB`)**.
     - Phân đoạn **`S0510`** (vĩ độ $\sim 0^\circ\text{N} - 12^\circ\text{N}$): Phủ **Tân Sơn Nhất (`VVTS`)**.
   - Dung lượng tải chỉ khoảng **$\sim 2.5\text{ MB}$/lượt quét** (thay vì $> 250\text{ MB}$ nếu tải toàn bộ đĩa và các kênh).
   - Có chính sách tự dọn tệp gốc `.DAT.bz2` quá hạn (`--retain-raw-hours 24`) sau khi đã trích xuất xong ảnh cắt PNG và chỉ số JSON, nhưng **luôn giữ lại (pin)** các tệp gốc thuộc cửa sổ phát lại (Replay window).
3. **Cắt vùng 2 Vòng (Dual-Box Geometry) bằng `Satpy`:**
   - **Khung ngữ cảnh ngoài (Outer Context Box — $\pm 1.0^\circ$ vĩ/kinh độ, $\approx 220\text{ km} \times 220\text{ km}$, $\sim 110 \times 110$ điểm ảnh):** Dùng để xuất ảnh PNG sắc nét giúp điều phối viên quan sát hướng di chuyển của dải mây quanh sân bay, đồng thời tính các chỉ số mây toàn vùng.
   - **Khung tại trạm (Inner Station Box — $\pm 0.25^\circ$ vĩ/kinh độ, $\approx 25\text{ km}$ quanh đường băng):** Được vẽ đánh dấu trực tiếp trên ảnh PNG và tính bộ chỉ số nhiệt độ đỉnh mây ngay trên đầu sân bay.
4. **Ngưỡng Vật lý & Chỉ báo Khí tượng Kênh B13:**
   - **Miền giá trị vật lý hợp lệ của Nhiệt độ sáng ($T_b$):** $[150.0\text{ K}, 340.0\text{ K}]$ (tương đương $[-123.15^\circ\text{C}, +66.85^\circ\text{C}]$). Mọi điểm ảnh ngoài khoảng này được coi là không hợp lệ khi tính `b13_valid_fraction`.
   - **Ngưỡng mây đối lưu lạnh (`cold_cloud_fraction`):** $T_b < 235.0\text{ K}$ ($-38.15^\circ\text{C}$).
   - **Ngưỡng đỉnh mây dông mạnh (`severe_convection_fraction`):** $T_b < 220.0\text{ K}$ ($-53.15^\circ\text{C}$).
5. **Đóng gói Cửa sổ Phát lại (Replay Window) & Kiểm thử Tệp Ảnh Hỏng:**
   - Tải trọn chuỗi ảnh 10 phút/lần cho cửa sổ mưa thật ngày `2026-11-09` từ **`06:00 UTC` đến `08:30 UTC`** (16 mốc quét liên tiếp) để bao phủ đầy đủ **chuỗi 6 ảnh (60 phút) trước sự kiện** cho cả **Ca 1 đáp án (`07:00 UTC` — mưa tại `VVDN`)** và **Ca đồng mức (`08:00 UTC` — mưa tại cả `VVDN` và `VVNB`)**.
   - Ghi riêng mọi lỗi tệp nhị phân `.DAT.bz2` bị hỏng/cắt cụt vào `data/logs/himawari_file_errors.jsonl`, kèm cờ `--inject-corrupt-test` tạo tệp lỗi có gắn nhãn `"is_synthetic_test": true` phục vụ nghiệm thu Buổi 6 & Buổi 8.

---

## 2. Kiến trúc Mở rộng và Cấu trúc Thư mục

```text
DLL/
├── docker-compose.yml                         # Bổ sung dịch vụ namenode, datanode (Hadoop 3) + ingestor/runner
├── Dockerfile                                 # Bổ sung thư viện satpy, pyresample, matplotlib, numpy, xarray
├── requirements.txt                           # Danh sách thư viện Python cho Mô-đun 1 + Mô-đun 2
├── config/
│   ├── airports.json                          # Bổ sung himawari_segment ("S0410"/"S0510"), outer_box_deg, inner_box_deg
│   └── hadoop/
│       └── core-site.xml & hdfs-site.xml      # Cấu hình Single-Node HDFS + bật WebHDFS REST API
├── src/
│   └── ingestion/
│       ├── hdfs_client.py                     # Giao tiếp WebHDFS (Dual-write + lệnh sync_to_hdfs + đo tốc độ ghi)
│       ├── himawari_client.py                 # Truy vấn S3 noaa-himawari9, tải phân đoạn B13 S0410 & S0510, dọn tệp cũ
│       ├── satpy_processor.py                 # Satpy đọc .DAT.bz2, cắt Dual-Box, tính chỉ số JSON, vẽ PNG & bắt lỗi tệp hỏng
│       └── run_ingest.py                      # Tích hợp --include-himawari, --himawari-window, --sync-hdfs, --inject-corrupt-test
├── tests/
│   └── ingestion/
│       ├── test_hdfs_client.py                # Unit test WebHDFS upload, sync_local_to_hdfs và fallback khi tắt HDFS
│       ├── test_himawari_client.py            # Unit test lọc phân đoạn S0410/S0510 từ S3 XML, tải atomic và dọn tệp cũ
│       └── test_satpy_processor.py            # Unit test tính b13_valid_fraction, ngưỡng 235K/220K, vẽ PNG & log tệp hỏng
└── data/
    ├── raw/
    │   ├── himawari_hsd/date=YYYY-MM-DD/slot=HHMM/
    │   │   └── HS_H09_YYYYMMDD_HHmm_B13_FLDK_R20_S0410.DAT.bz2
    │   └── himawari_metrics/date=YYYY-MM-DD/
    │       └── himawari_metrics_<scan_slot>_<batch_id>.json
    ├── processed/
    │   └── himawari_png/airport=<ICAO>/date=YYYY-MM-DD/
    │       └── B13_<ICAO>_<YYYYMMDD_HHMM>Z.png
    └── logs/
        ├── ingestion_log.jsonl                # Nhật ký thu thập chung (METAR, Open-Meteo, Himawari S3)
        ├── himawari_file_errors.jsonl         # Nhật ký lỗi tệp ảnh nhị phân riêng (bắt buộc cho Buổi 8)
        └── hdfs_sync_log.jsonl                # Nhật ký đo tốc độ & dung lượng đồng bộ HDFS (cho Ma trận lưu trữ Buổi 6)
```

---

## 3. Thiết kế Chi tiết Từng Thành phần

### 3.1. Cấu hình Sân bay Mở rộng (`config/airports.json`)

Mỗi sân bay trong `config/airports.json` được bổ sung thông tin định vị vệ tinh:
- **`VVDN` (Đà Nẵng — $16.0439^\circ\text{N}, 108.1994^\circ\text{E}$):**
  - `himawari_segment`: `"S0410"`
  - `outer_half_deg`: `1.0` (khung $[107.1994, 15.0439, 109.1994, 17.0439]$)
  - `inner_half_deg`: `0.25` (khung $[107.9494, 15.7939, 108.4494, 16.2939]$)
- **`VVNB` (Nội Bài — $21.2212^\circ\text{N}, 105.8072^\circ\text{E}$):**
  - `himawari_segment`: `"S0410"`
  - `outer_half_deg`: `1.0`, `inner_half_deg`: `0.25`
- **`VVTS` (Tân Sơn Nhất — $10.8188^\circ\text{N}, 106.6520^\circ\text{E}$):**
  - `himawari_segment`: `"S0510"`
  - `outer_half_deg`: `1.0`, `inner_half_deg`: `0.25`

### 3.2. Tầng Tích hợp Hadoop HDFS (`src/ingestion/hdfs_client.py`)
- **Cấu hình Cụm Docker (`docker-compose.yml`):**
  - Sử dụng image `apache/hadoop:3` cho `namenode` và `datanode`.
  - Cổng mở ra máy chủ: `9870:9870` (NameNode UI), `9000:9000` (IPC cho Spark `hdfs://namenode:9000`), `9864:9864` (DataNode WebHDFS).
  - Thiết lập `dfs.replication=1`, `dfs.webhdfs.enabled=true`, `dfs.permissions.enabled=false` để tránh lỗi phân quyền user giữa Windows và container Linux.
- **Giao thức Ghi Kép & Đồng bộ (`WebHDFSClient`):**
  - `is_hdfs_available() -> bool`: Kiểm tra nhanh endpoint `http://<host>:9870/webhdfs/v1/?op=GETFILESTATUS` với timeout `1.5s`.
  - `upload_file(local_path: str, hdfs_path: str) -> dict`: Đẩy tệp lên HDFS qua WebHDFS (`PUT ...?op=CREATE&overwrite=true`). Nếu HDFS đang tắt, trả về trạng thái `"SKIPPED_HDFS_OFFLINE"` mà không làm gián đoạn quá trình thu thập cục bộ.
  - `sync_directory_to_hdfs(local_root: str = "data", hdfs_root: str = "/data") -> dict`: Quét toàn bộ tệp trong `data/raw/`, `data/processed/`, `data/logs/`, đẩy các tệp chưa có (hoặc mới hơn) lên HDFS, đồng thời ghi số tệp, tổng số byte và thời gian thực thi (`elapsed_ms`, `throughput_mb_s`) vào `data/logs/hdfs_sync_log.jsonl`.

### 3.3. Bộ Tải Phân đoạn Himawari-9 từ AWS S3 (`src/ingestion/himawari_client.py`)
- **Nguồn dữ liệu:** Bucket công khai `https://noaa-himawari9.s3.amazonaws.com`
- **Cấu trúc khóa S3 (Prefix):** `AHI-L1b-FLDK/YYYY/MM/DD/HHmm/` (trong đó `HHmm` là bội số của 10 phút: `0000, 0010, ..., 2350`).
- **Quy tắc dò Slot mới nhất:**
  - Vì vệ tinh Himawari-9 cần khoảng $12 - 18$ phút sau giờ quét để tải đầy đủ các phân đoạn lên AWS S3, khi chạy ở chế độ thời gian thực (`--once` hoặc `--loop`), client sẽ kiểm tra lùi từ `now_utc - 15 phút` (làm tròn xuống 10 phút) và lùi tối đa 3 slot nếu slot mới nhất chưa đủ 2 phân đoạn `S0410` và `S0510`.
- **Tên tệp cần lọc:**
  - Kênh `B13`, phân đoạn `S0410` và `S0510`:
    `HS_H09_<YYYYMMDD>_<HHmm>_B13_FLDK_R20_S0410.DAT.bz2`
    `HS_H09_<YYYYMMDD>_<HHmm>_B13_FLDK_R20_S0510.DAT.bz2`
- **Lưu trữ nguyên tử & Đối soát độ phủ:**
  - Tải về `data/raw/himawari_hsd/date=YYYY-MM-DD/slot=HHMM/<filename>.tmp` rồi đổi tên nguyên tử sang `.DAT.bz2`.
  - Ghi vào `data/logs/ingestion_log.jsonl` với các trường đối soát: `expected_segments: 2`, `downloaded_segments: <0..2>`, `bytes_written`, `status` (`SUCCESS` khi đủ 2 phân đoạn, `PARTIAL` khi chỉ có 1 phân đoạn, `FAILED` khi 0 phân đoạn).
- **Chính sách Dọn tệp Gốc (Retention Policy):**
  - Hàm `cleanup_old_raw_segments(raw_hsd_dir, retain_hours=24, pinned_slots=[...])`: Xóa các tệp `.DAT.bz2` đã cũ hơn `retain_hours` **chỉ khi** slot đó đã có tệp JSON chỉ số trong `data/raw/himawari_metrics/` và không nằm trong danh sách `pinned_slots` (như cửa sổ phát lại `2026-11-09 06:00–08:30Z`).

### 3.4. Bộ Đọc `Satpy`, Trích Chỉ số Dual-Box & Xuất Ảnh PNG (`src/ingestion/satpy_processor.py`)
- **Bước 1 — Kiểm tra toàn vẹn tệp nhị phân `.DAT.bz2`:**
  - Trước và trong khi đưa vào `satpy.Scene(filenames=[...], reader="ahi_hsd")`, kiểm tra giải nén luồng `bz2` và kích thước header HSD.
  - Nếu tệp bị hỏng, cắt cụt hoặc `Satpy` báo lỗi đọc tệp, lập tức ghi 1 bản ghi vào `data/logs/himawari_file_errors.jsonl`:
    ```json
    {
      "error_id": "20261109T080000Z_err_9f8a1b",
      "logged_at_utc": "2026-11-09T09:05:12Z",
      "scan_slot_utc": "2026-11-09T08:00:00Z",
      "segment": "S0410",
      "file_path": "data/raw/himawari_hsd/date=2026-11-09/slot=0800/HS_H09_20261109_0800_B13_FLDK_R20_S0410.DAT.bz2",
      "file_size_bytes": 128,
      "error_type": "CorruptBZ2OrHSDError",
      "error_message": "Compressed file ended before the end-of-stream marker was reached",
      "is_synthetic_test": true
    }
    ```
- **Bước 2 — Định vị và Cắt vùng Dual-Box theo Sân bay:**
  - Tải dataset `scn.load(["B13"])`.
  - Lấy lưới tọa độ địa lý `(lons, lats) = scn["B13"].attrs["area"].get_lonlats()` và mảng giá trị `bt_kelvin = scn["B13"].values` (hoặc dùng `scn.crop(ll_bbox=...)`).
  - Lọc điểm ảnh trong **Khung ngoài ($\pm 1.0^\circ$)** và **Khung trong tại trạm ($\pm 0.25^\circ$)**:
    - Kiểm tra điều kiện vật lý hợp lệ: $150.0 \le T_b \le 340.0\text{ K}$ và không phải `NaN`.
    - Tính `b13_valid_fraction` = `(số điểm hợp lệ trong khung ngoài) / (tổng số điểm ảnh trong khung ngoài)`.
    - Tính cho cả khung trong (`inner_*`) và khung ngoài (`outer_*`):
      - `bt_min_k`, `bt_min_c` ($T_b - 273.15$)
      - `bt_mean_k`, `bt_mean_c`
      - `cold_cloud_fraction`: tỷ lệ điểm ảnh hợp lệ có $T_b < 235.0\text{ K}$ ($-38.15^\circ\text{C}$).
      - `severe_convection_fraction`: tỷ lệ điểm ảnh hợp lệ có $T_b < 220.0\text{ K}$ ($-53.15^\circ\text{C}$).
- **Bước 3 — Vẽ Ảnh PNG Đã Chú giải (`data/processed/himawari_png/airport=<ICAO>/date=YYYY-MM-DD/`):**
  - Sử dụng `matplotlib` vẽ lưới nhiệt độ sáng $T_c$ ($^\circ\text{C}$) của khung ngoài $\pm 1.0^\circ$ với bảng màu chuẩn hồng ngoại (mây càng lạnh/cao càng nổi bật).
  - Đánh dấu tâm sân bay (`+` + mã `VVDN`/`VVNB`/`VVTS`), vẽ khung nét đứt $\pm 0.25^\circ$ quanh sân bay, tiêu đề ghi rõ giờ quét `Observed UTC`, và dòng chữ chân trang bắt buộc theo điều khoản JMA/NOAA:
    `"Processed Product | Source: JMA/NOAA Himawari-9 B13 via AWS Open Data | Cropped & Annotated by Group"`.
- **Bước 4 — Lưu Phong bì Chỉ số JSON (`data/raw/himawari_metrics/date=YYYY-MM-DD/`):**
  - Lưu tệp JSON chứa `metadata` và `payload` (mảng 3 bản ghi ứng với `VVDN`, `VVNB`, `VVTS`) để PySpark ở Mô-đun 3 đọc trực tiếp theo schema.

---

## 4. Ma trận Lưu trữ Mở rộng (Chuẩn bị cho Báo cáo Buổi 6 — CLO2)

Với việc bổ sung Hadoop HDFS ở chế độ Dual-Mode, bảng Ma trận lưu trữ trong báo cáo của nhóm sẽ so sánh 3 phương án dựa trên số đo thực tế từ `ingestion_log.jsonl` và `hdfs_sync_log.jsonl`:

| Tiêu chí | Kho tệp cục bộ + Parquet | Hadoop HDFS (Single-Node Docker) + Parquet | PostgreSQL (Phương án đối chiếu) |
|---|---|---|---|
| **Loại dữ liệu & Workload** | Lưu trực tiếp cả tệp nhị phân `.DAT.bz2`, ảnh `.png`, JSON gốc và bảng Parquet phân vùng `date=YYYY-MM-DD`. | Chuẩn Data Lake phân tán: lưu nguyên bản các phân vùng `date=YYYY-MM-DD` trên HDFS, giao tiếp tự nhiên với PySpark (`hdfs://namenode:9000`). | Mạnh về truy vấn quan hệ OLTP; không phù hợp lưu trực tiếp hàng nghìn tệp vệ tinh nhị phân `.DAT.bz2`. |
| **Độ trễ & Hiệu năng** | Độ trễ đọc/ghi thấp nhất trên 1 máy (không qua tầng mạng container). Làm bản dự phòng tức thời khi demo. | Có thêm độ trễ truyền qua WebHDFS/DataNode (được đo thực tế trong `hdfs_sync_log.jsonl`), nhưng sẵn sàng cho đọc song song nhiều DataNode. | Nhanh khi truy vấn điểm có chỉ mục, chậm hơn khi quét xử lý hàng loạt theo lô lớn. |
| **Khả năng mở rộng** | Giới hạn bởi dung lượng ổ cứng của 1 máy cá nhân. | Mở rộng ngang bằng cách thêm container/máy `datanode` và tăng `dfs.replication` mà không phải sửa code PySpark. | Cần thiết kế sharding/partitioning phức tạp khi dữ liệu phình to. |
| **Chi phí tài nguyên (RAM/Disk)** | Chiếm ~0 MB RAM nền; chỉ tốn dung lượng đĩa thực tế. | Tiêu thụ thêm ~1.5 GB RAM cho JVM của NameNode + DataNode. Do đó nhóm áp dụng **Dual-Mode linh hoạt**. | Tiêu thụ ~200–500 MB RAM nền cho dịch vụ CSDL và chi phí quản trị schema/backup. |

