# Hệ thống hiện tại: đọc xong là hiểu toàn bộ

Dành cho mọi thành viên nhóm (A, B, C), kể cả người không viết mã. Cập nhật 2026-11-17, theo mã nguồn và tài liệu ở commit `437144a` trở đi.
Nếu tài liệu này mâu thuẫn với `README.md`, tin tài liệu này: mục "Cấu trúc Dự án" trong README vẫn ghi dashboard 3 tab và số test cũ, chưa cập nhật.
Kết quả đo và số liệu chạy thật nằm ở `handoff-nhom.md` và thư mục `ket-qua/`.

## 1. Hệ thống làm gì và cho ai

**Người dùng:** điều phối viên rà soát thời tiết.
**Câu hỏi họ cần trả lời nhanh:** tại thời điểm này, trong 3 sân bay (Đà Nẵng VVDN, Nội Bài VVNB, Tân Sơn Nhất VVTS), sân bay nào cần rà soát trước, vì sao, và bằng chứng gốc là gì?
**Cách hệ thống trả lời:** thu thập 3 nguồn dữ liệu, kiểm tra và làm sạch, ghép đúng theo thời điểm, áp 4 mức ưu tiên, hiển thị trên dashboard cùng bản tin gốc và ảnh mây.

Ba nguồn dữ liệu (đề bài giới hạn đúng 3 nguồn):

| Nguồn | Nội dung | Nhịp thu | Lấy từ đâu |
|---|---|---|---|
| METAR | Bản tin quan trắc thực tế tại sân bay: mưa, dông, tầm nhìn, bản tin gốc `rawOb` | 10 phút | NOAA Aviation Weather Center (API) |
| Open-Meteo | Dự báo theo giờ: lượng mưa, tầm nhìn, gió | 60 phút | Open-Meteo (API) |
| Himawari-9 kênh B13 | Ảnh hồng ngoại mây quanh sân bay; từ đó tính nhiệt độ đỉnh mây | 10 phút | Kho công cộng `noaa-himawari9` trên AWS S3 |

## 2. Bức tranh tổng thể

```text
 [METAR]  [Open-Meteo]  [Himawari-9 S3]
     \          |          /
      v         v         v
   ┌──────────────────────────┐
   │ 1. THU NHẬN (ingestor)   │  Python, chạy liên tục
   └────────────┬─────────────┘
                v
   ┌──────────────────────────┐
   │ BRONZE: dữ liệu thô      │  data/raw_compact (JSONL), ảnh PNG, .DAT.bz2
   └────────────┬─────────────┘
                v
   ┌──────────────────────────┐
   │ 2. XỬ LÝ (Spark)         │  đọc theo schema, loại bản ghi lỗi,
   │                          │  ghép đúng thời điểm, xếp mức 1–4
   └────────────┬─────────────┘
                v
   ┌──────────────────────────┐
   │ SILVER + GOLD (Parquet)  │  data/lake/silver, data/lake/gold
   └──────┬────────────┬──────┘
          v            v
 ┌────────────────┐  ┌──────────────────────────┐
 │ 3. DASHBOARD   │  │ 4. PHÂN TÍCH NÂNG CAO    │ ghi lại vào Gold
 │ (Streamlit,    │  │ sức khỏe · kiểm định ·   │
 │ chỉ đọc Gold)  │  │ dự đoán 0–60′            │
 └────────────────┘  └──────────────────────────┘

 Tùy chọn: đồng bộ Bronze sang HDFS (Hadoop) và/hoặc ghi lake lên HDFS.
```

Ba ý cần nhớ:
1. **Kiểu Medallion (Bronze → Silver → Gold):** Bronze giữ nguyên dữ liệu gốc; Silver là dữ liệu đã kiểm tra và chuẩn hóa; Gold là bảng cuối dùng để hiển thị và chấm điểm.
2. **Spark là nơi duy nhất tính mức ưu tiên** (ADR-003). Dashboard chỉ đọc kết quả, không tự tính.
3. **Mọi bảng Silver/Gold là Parquet có phiên bản**: mỗi lần chạy ghi một thư mục `run_id=...` mới, chỉ hiển thị khi có tệp `_SUCCESS`, giữ 2 phiên bản mới nhất. Nhờ vậy dashboard không bao giờ đọc nửa chừng.

## 3. Từng khối hoạt động thế nào

### 3.1 Thu nhận (`src/ingestion/`)

Điểm vào: `python -m src.ingestion.run_ingest` (`--once` chạy một vòng, `--loop` chạy liên tục, `--include-himawari` thêm ảnh vệ tinh). Trên Windows: nhấp đúp `run_daemon.bat`.

| Tệp | Việc làm |
|---|---|
| `metar_client.py`, `open_meteo_client.py` | Gọi API, giữ nguyên bản tin gốc |
| `himawari_client.py` | Tải 2 phân đoạn ảnh mỗi lượt: `S0410` (phủ VVDN và VVNB) và `S0510` (phủ VVTS). Luôn đủ cả hai |
| `satpy_processor.py` | Dùng thư viện Satpy cắt vùng quanh sân bay, tính chỉ số mây, vẽ ảnh PNG |
| `http_client.py` | Gọi mạng có thử lại và chờ tăng dần |
| `raw_store.py`, `storage.py` | Ghi Bronze nguyên tử (ghi tệp tạm rồi đổi tên) nên mất điện không hỏng dữ liệu; có khóa liên tiến trình |
| `metar_backfill.py`, `import_metrics.py` | Tải bù METAR tối đa 30 ngày (giới hạn của AWC) và nhập chỉ số Himawari đã tính sẵn |
| `hdfs_client.py` | Đồng bộ sang HDFS qua WebHDFS và đo thông lượng |
| `migrate_storage.py` | Chuyển layout cũ (nhiều tệp lẻ) sang layout gom ngày |

Chi tiết cần biết:
- **Mỗi bản ghi mang một "phong bì"** (batch_id, nguồn, thời điểm nhận, `is_backfill`...). Từ đó hệ thống luôn biết một dữ liệu được lấy về lúc nào, khác với lúc nó xảy ra.
- **Bronze layout `daily_jsonl`:** một tệp `batches-0001.jsonl` mỗi nguồn mỗi ngày, tại `data/raw_compact/<nguồn>/date=YYYY-MM-DD/`. Mỗi lượt thu ghi lại chính tệp cuối của ngày đó. Hệ quả: Spark không dùng được chế độ "đọc luồng theo tệp" (ADR-004).
- **Tệp ảnh thô `.DAT.bz2` nặng** (khoảng 5,67 MB mỗi lượt, 816 MB/ngày). Sau khi trích chỉ số, tệp thô bị xóa (daemon giữ 2 giờ, `--retain-raw-hours`), trừ các mốc thuộc cửa sổ phát lại đã ghim.
- **Tệp ảnh hỏng** bị cô lập và ghi vào `data/logs/himawari_file_errors.jsonl`, đếm riêng theo tệp.
- **Nhật ký kiểm toán:** `data/logs/ingestion_log.jsonl` (số lượt quét, số bản ghi, số byte, độ phủ).

### 3.2 Bronze: dữ liệu thô

| Thành phần | Vị trí |
|---|---|
| METAR, Open-Meteo, chỉ số Himawari | `data/raw_compact/<nguồn>/date=YYYY-MM-DD/batches-0001.jsonl` |
| Ảnh PNG đã vẽ | `data/processed/himawari_png/airport=<ICAO>/date=YYYY-MM-DD/` |
| Tệp thô vệ tinh | `data/raw/himawari_hsd/` (gần như luôn rỗng vì bị dọn) |
| Trạng thái layout | `data/storage_state.json` (`daily_jsonl`) |

`data/raw/<nguồn>/date=.../` hiện rỗng: dữ liệu đã được gom sang `raw_compact`. Đừng nhầm.

### 3.3 Xử lý bằng Spark (`src/spark/`)

Điểm vào: `python -m src.spark.run_pipeline --storage-root data`. Chạy một lần mất khoảng 1 phút (lạnh 52 s, ấm 18 s). Chế độ `--loop` giữ một SparkSession, cứ 15 giây kiểm tra Bronze có đổi không, đổi thì công bố lại Gold (ADR-004).

Các bước, theo thứ tự:

| Bước | Tệp | Việc làm |
|---|---|---|
| 1. Đọc theo schema | `schemas.py`, `readers.py` | Định nghĩa kiểu bắt buộc cho từng nguồn; dòng hỏng được giữ lại ở cột `_corrupt_record` thay vì làm sập cả lượt |
| 2. Tách bản ghi lỗi | `quarantine.py` | Dòng sai kiểu, sai sân bay, sai giờ, giá trị vật lý vô lý (nhiệt độ sáng B13 ngoài 150–340 K, tỷ lệ ngoài 0–1) đi vào bảng `quarantine` kèm mã lỗi. Đảm bảo luôn đúng: **số dòng đọc = số dòng hợp lệ + số dòng bị loại** |
| 3. Chuẩn hóa | `transforms.py` | Tầm nhìn về mét (9999 = từ 10 km trở lên); nhận diện mưa/dông tại trạm (như `RA`, `TSRA`, `SHRA`) khác với hiện tượng lân cận (`VC...`) và phần xu hướng (`TEMPO`, `BECMG`); gắn nhãn mây B13: lõi đối lưu mạnh khi nhiệt độ đỉnh mây dưới 220 K, mây lạnh khi tỷ lệ mây lạnh từ 0,05 hoặc dưới 235 K |
| 4. Ghép đúng thời điểm | `temporal_join.py` | Xem mục 4 bên dưới |
| 5. Xếp mức ưu tiên | `priority_rules.py`, `priority_engine.py` | Xem mục 5 bên dưới |
| 6. Ghi kết quả | `writers.py`, `lake_paths.py` | Ghi Parquet có phiên bản `run_id`, `_SUCCESS` làm dấu hoàn tất, giữ 2 bản mới nhất; ghi được vào ổ đĩa hoặc `hdfs://` |

Các tệp khác: `session.py` (khởi tạo Spark, hợp với cả đĩa và HDFS), `runner.py` (vòng chạy lâu dài, ghi nhịp tim vào `data/lake/_runner_heartbeat.json`), `replay_slots.py` (đọc `config/replay_slots.json`), `himawari_batch.py` (xử lý hàng loạt ảnh vệ tinh song song bằng Spark, dùng cho tải bù).

**Chế độ chạy:** `--mode replay` (phát lại các mốc cố định), `--mode live` (mốc thực tế), `--review-slots all` (mặc định: mốc phát lại cộng mọi mốc live; mốc live lấy từ giờ nhận METAR làm tròn lên 5 phút), `auto` (mốc phát lại cộng mốc live mới nhất).

### 3.4 Lake: bảng nào nằm ở đâu

```text
data/lake/
├── silver/
│   ├── metar_observations/       run_id=.../date=.../*.parquet
│   ├── open_meteo_forecasts/     ...
│   ├── himawari_b13_metrics/     ...
│   └── quarantine/               bản ghi bị loại + mã lỗi
├── gold/
│   ├── decision_table/           BẢNG QUYẾT ĐỊNH (3 dòng mỗi mốc)
│   ├── reconciliation/           đối soát số dòng
│   ├── verification/             kiểm định quy tắc
│   ├── feature_table/            bảng đặc trưng cho mô hình
│   ├── nowcast/                  dự đoán 0–60′
│   └── health/*.parquet, summary.json   chẩn đoán vận hành
└── _runner_heartbeat.json
models/<run_id>/                  mô hình đã huấn luyện + metadata.json
```

Từ điển từng cột (6 thuộc tính mỗi trường): `docs/architecture/05-data-dictionary.md`.

### 3.5 Dashboard (`src/dashboard/`)

Chạy: nhấp đúp `run_dashboard.bat` (hoặc `streamlit run src/dashboard/app.py`), mở `http://localhost:8501`. **Chỉ đọc Gold**, nạp dưới 1 giây, dùng được offline.

| Tab | Nội dung |
|---|---|
| 1. Quyết định | Ba sân bay theo thứ tự ưu tiên, giờ UTC và giờ Việt Nam song song, banner cam khi đồng mức đứng đầu, nút đảo thứ tự hiển thị (không đổi kết quả). Bấm một sân bay để truy nguyên: bản tin METAR gốc, dự báo mưa và giờ hiệu lực, chuỗi tối đa 6 ảnh B13 (10 phút mỗi ảnh) |
| 2. Đo KPI | Bộ bấm giờ cho người thử, so hai cách xem (xem 3 nguồn rời so với xem dashboard), tự chấm điểm, tính % rút ngắn thời gian trung vị (mục tiêu từ 30%). Lưu `data/logs/kpi_trials_log.jsonl` |
| 3. Đối soát dữ liệu | Bảng bảo toàn dòng 3 nguồn, trình xem bản ghi bị loại, lỗi tệp nhị phân, thống kê HDFS |
| 4. Sức khỏe hệ thống | Độ mới từng nguồn, độ phủ lượt quét, nguyên nhân khoảng thiếu, bất thường, trôi dữ liệu, độ trễ công bố, thời gian từng giai đoạn Spark |

Hai chế độ ở thanh bên: **LIVE** (mốc thực tế; có ô báo "Có dữ liệu mới" cập nhật mỗi 60 giây, bấm "Xem mốc mới" để nhảy sang mốc mới nhất; tạm dừng khi đang đo KPI) và **REPLAY** (3 mốc cố định `2026-11-09T07:00Z`, `08:00Z`, `11:00Z` dùng cho demo và đo KPI). Nút "Cập nhật dữ liệu realtime" chạy thu nhận rồi chạy pipeline một lần khi không có runner.

Tệp chính: `app.py` (khung), `data_loader.py` (đọc Gold, có bộ nhớ đệm), `views/` (từng tab), `kpi_evaluator.py`, `metar_decoder.py` (giải mã bản tin cho người đọc), `b13_viewer.py` (chuỗi ảnh).

### 3.6 Phân tích nâng cao

Ba trụ cột bổ sung, đều ghi lại vào Gold:

| Trụ cột | Mã | Trả lời câu hỏi |
|---|---|---|
| Chẩn đoán vận hành | `src/diagnostics/` (`run_health.py`) | Hệ thống có chạy đúng không? Thiếu dữ liệu do đâu? Phân loại `SOURCE_MISSING` (tệp không có trên S3) và `RECEIVER_FAULT` (máy thu ngừng); bất thường bằng MAD; trôi dữ liệu bằng PSI; độ trễ P50/P95 |
| Kiểm định quy tắc | `src/analytics/verification.py` | Dự báo mưa và thứ tự Mức 1–4 có khớp thực tế không? Bảng 2×2 (POD, FAR, CSI) tại ngưỡng 0,1/0,5/1,0 mm; tỷ lệ sự kiện theo mức, kiểm tra Mức 1 ≥ Mức 3 ≥ Mức 4 |
| Dự đoán 0–60′ | `src/ml/` (`labels`, `features`, `train`, `evaluate`, `infer`) | Sân bay có chuyển lên Mức 1 trong 60 phút tới không? Logistic Regression và GBT của Spark MLlib, hiệu chỉnh Platt, chia theo thời gian có khoảng đệm, so với 3 cách đơn giản. **Chỉ hiển thị khi thắng cả 3** (ADR-006) |

## 4. Quy tắc ghép đúng thời điểm (không rò rỉ tương lai)

Đây là phần dễ hiểu sai nhất, và là phần được kiểm thử kỹ nhất.

**Khái niệm:** tại mỗi "mốc rà soát" `T`, hệ thống chỉ được dùng những gì **đã có trong tay trước `T`**. Hai thời điểm khác nhau:
- thời điểm **xảy ra** (METAR quan trắc lúc nào, giờ hiệu lực của dự báo);
- thời điểm **nhận được** (hệ thống lấy về lúc nào).

Quy tắc cụ thể:
- **METAR:** lấy bản mới nhất có giờ quan trắc ≤ `T`. Ở chế độ live thì bản đó còn phải được lấy về trước `T`. Bản tin được sửa (revision 2) chỉ xuất hiện từ lúc nó được lấy về; trước đó vẫn dùng bản cũ.
- **Dự báo:** luôn phải được lấy về ≤ `T`, ở mọi chế độ. Dự báo lấy về sau `T` không được dùng. Dữ liệu tải bù lịch sử không có dự báo, nên kết quả hiện "dự báo khuyết", không lấp.
- **Ảnh B13:** lượt quét gần nhất ≤ `T`.
- **Luôn đúng 3 dòng mỗi mốc**, kể cả khi một nguồn hoàn toàn thiếu (sân bay vẫn có dòng, các cột thiếu ghi "không có").
- Chạy lại cùng đầu vào phải cho cùng khóa và cùng giá trị.

Dòng thời gian khác cần nhớ khi đọc kết quả: lượng mưa Open-Meteo là **tổng của giờ liền trước** `(T−1h, T]`, không phải giờ sắp tới. Đây là cách từ điển đang ghi; chưa đối chiếu lại với tài liệu chính thức của Open-Meteo.

## 5. Bốn mức ưu tiên (đáp án chấm KPI)

Áp theo thứ tự, mức đầu tiên khớp thì dừng. Ngưỡng nằm tại `src/spark/priority_rules.py` (nguồn duy nhất; có test kiểm tra Spark và bản Python cho kết quả y hệt).

| Mức | Điều kiện | Lý do hiển thị (tiếng Việt) |
|---|---|---|
| 1 | Có METAR còn mới (không quá 60 phút) **và** (có mưa/dông tại trạm **hoặc** tầm nhìn dưới 5000 m) | "METAR mới (08:00Z) ghi nhận mưa/dông tại trạm (...)" |
| 2 | Không phải Mức 1 và (thiếu METAR **hoặc** METAR cũ hơn 60 phút) | "Cần xác minh dữ liệu: ..." |
| 3 | Không phải Mức 1, 2 và có dự báo, mưa giờ tới ≥ 0,1 mm | "Điều kiện tại trạm tốt, nhưng dự báo mưa giờ tới đạt ..." |
| 4 | Còn lại | "Điều kiện tại trạm bình thường, không có cảnh báo mưa giờ tới" |

**Đồng mức:** nếu nhiều sân bay cùng đứng mức nhỏ nhất của mốc đó, tất cả đều là đáp án đúng (cột `accepted_first_review_airports`, cờ `is_co_priority_top`); dashboard hiện banner. Ví dụ mốc 08:00 phát lại: VVDN và VVNB cùng Mức 1.

Một METAR cũ mà đang có dông vẫn là **Mức 2**, không phải Mức 1: bản tin quá cũ thì phải xác minh trước.

## 6. Quản lý tiến trình: ai chạy gì, khi nào

| Tiến trình | Lệnh hoặc tệp | Khi nào |
|---|---|---|
| Thu nhận liên tục | `run_daemon.bat` | Bật suốt thời gian thu dữ liệu (cần để có số đo 24 giờ) |
| Spark runner | `run_pipeline --loop` hoặc `docker compose --profile spark up -d spark-runner` | Khi demo LIVE |
| Pipeline một lần | `run_pipeline --storage-root data` | Sau khi thu xong; để dựng lại Gold |
| Chẩn đoán | `python -m src.diagnostics.run_health` | Trước khi xem tab Sức khỏe |
| Huấn luyện / suy luận | `src.ml.train`, `src.ml.infer` | Khi có thêm dữ liệu |
| Dashboard | `run_dashboard.bat` | Khi cần xem |
| HDFS (tùy chọn) | `docker compose up -d namenode datanode`, giao diện `http://localhost:9870` | Khi muốn minh họa phân tán (tốn khoảng 1,5 GB RAM) |
| Đóng gói bộ mẫu | `python scripts/package_sample.py --storage-root data --out dist/sample_dataset.zip` | Trước khi nộp |

Docker Compose (`docker-compose.yml`) có các dịch vụ: `ingestor`, `dashboard`, `namenode`, `datanode`, và (theo profile) `spark-processor`, `spark-runner`, `runner`.

## 7. Thư mục dự án (bản đồ)

```text
DLL/
├── config/             airports.json (tọa độ, phân đoạn vệ tinh), replay_slots.json (3 mốc phát lại)
├── src/
│   ├── ingestion/      thu nhận
│   ├── spark/          xử lý, quy tắc ưu tiên, ghi lake
│   ├── dashboard/      giao diện, KPI
│   ├── diagnostics/    chẩn đoán vận hành
│   ├── analytics/      kiểm định quy tắc
│   └── ml/             dự đoán 0–60′
├── data/
│   ├── raw_compact/    Bronze
│   ├── lake/           Silver, Gold
│   ├── processed/      ảnh PNG, decision_table.jsonl cũ (bản dự phòng ngoại tuyến)
│   └── logs/           ingestion_log, himawari_file_errors, hdfs_sync_log, pipeline_runs, health_runs, kpi_trials_log
├── models/             mô hình đã huấn luyện
├── tests/              bộ kiểm thử (333 test)
├── scripts/            package_sample.py
├── dist/               sample_dataset.zip (bộ mẫu nộp)
├── docs/
│   ├── architecture/   01 hiện trạng · 02 đánh giá · 03 kiến trúc đích · 04 roadmap · 05 từ điển · adr/
│   ├── superpowers/plans/   kế hoạch đã triển khai
│   └── handoff/        tài liệu này, handoff-nhom.md, checklist Excel, ket-qua/
└── .harness/           số đo và log thử nghiệm (không lưu git)
```

## 8. Giới hạn và điều dễ hiểu nhầm

1. **Dữ liệu nhỏ (MB).** Lập luận "Big Data" của dự án dựa vào đa dạng nguồn, tốc độ thu, độ tin cậy và khả năng mở rộng, không phải dung lượng.
2. **Chạy một máy cá nhân.** Spark chạy `local[*]`; không phải cụm. HDFS chỉ một NameNode và một DataNode, `replication=1`, không an toàn dữ liệu thật.
3. **Mô hình dự đoán chỉ tham khảo** và hiện không qua cổng chất lượng nên không hiển thị. Mức 1–4 không bị nó thay đổi.
4. **Dự báo chỉ do hệ thống tự thu.** Không có API dự báo lịch sử; ca lịch sử chưa có dự báo thì ghi "khuyết".
5. **Dữ liệu tải bù khác dữ liệu live.** Có cờ `is_backfill`; bộ mẫu nộp không chứa dữ liệu tải bù.
6. **Mới chỉ có tháng 11 (mùa khô).** Nhãn mưa chủ yếu ở VVDN, nên các điểm số theo sân bay không đồng đều.
7. **Máy thu từng ngừng nhiều ngày** (11-11 đến 11-15), nên dữ liệu live có khoảng trống; tab Sức khỏe hiển thị điều này.
8. **Windows hay lỗi Spark:** kết nối Python worker đôi khi thất bại khi chạy cả bộ test.
9. **Ổ đĩa hạn chế.** Ổ E: lúc đo còn khoảng 226 MB trống. Kiểm tra trước khi thu liên tục.
10. **Mức 2 chưa có ca thật** trong dữ liệu hiện có.

## 9. Từ điển thuật ngữ

| Thuật ngữ | Nghĩa |
|---|---|
| METAR | Bản tin quan trắc thời tiết sân bay, thường 30 phút một bản, ở đây thu mỗi 10 phút |
| SPECI | METAR đặc biệt khi thời tiết đổi nhanh; AWC không trả cho 3 trạm này |
| Himawari-9, B13 | Vệ tinh địa tĩnh Nhật Bản; kênh B13 là hồng ngoại sạch 10,4 µm, thấy đỉnh mây lạnh (mây dông) |
| HSD, `.DAT.bz2` | Định dạng tệp phân đoạn ảnh thô của Himawari, nén bz2 |
| Dual-Box | Hai ô quanh sân bay: ngoài ±1,0°, trong ±0,25° |
| Bronze / Silver / Gold | Ba lớp dữ liệu: thô / đã làm sạch / bảng cuối |
| Parquet | Định dạng bảng nén theo cột, Spark đọc nhanh |
| Quarantine | Nơi chứa bản ghi lỗi kèm mã lỗi, không bị xóa im lặng |
| Đối soát (reconciliation) | Kiểm tra số dòng vào bằng số dòng ra cộng số dòng loại |
| Mốc rà soát (review slot) | Thời điểm `T` tại đó đưa ra quyết định |
| Replay | Phát lại mốc cố định trong quá khứ; Live: mốc thực tế |
| Point-in-time join | Ghép dữ liệu chỉ dùng những gì đã có trước mốc |
| Backfill (tải bù) | Tải dữ liệu quá khứ cho đủ lịch sử |
| Đồng mức (co-priority) | Nhiều sân bay cùng mức đứng đầu |
| POD / FAR / CSI | Tỷ lệ bắt trúng / báo nhầm / điểm tổng hợp (càng cao càng tốt, trừ FAR) |
| PSI, MAD | Chỉ số đo trôi dữ liệu / ngưỡng phát hiện bất thường |
| Persistence | Cách dự đoán đơn giản "giờ tới giống giờ trước" |
| WebHDFS, HDFS | Kho tệp phân tán của Hadoop và giao diện truy cập qua HTTP |
| ADR | Bản ghi một quyết định kiến trúc kèm lý do; có 6 bản trong `docs/architecture/adr/` |

## 10. Đọc tiếp

| Muốn biết | Mở |
|---|---|
| Số đo, kết quả, rủi ro, việc còn lại | `handoff-nhom.md`, `ket-qua/` |
| Việc của từng người | `checklist-cong-viec.xlsx` |
| Vì sao chọn thiết kế này | `docs/architecture/adr/` (6 quyết định) |
| Hiện trạng trước khi sửa, các điểm lệch | `docs/architecture/01-as-is.md` |
| Đánh giá 5V, nút thắt, ma trận lưu trữ | `docs/architecture/02-assessment.md` |
| Lộ trình mở rộng | `docs/architecture/04-roadmap.md` |
| Từ điển dữ liệu từng cột | `docs/architecture/05-data-dictionary.md` |
| Mô tả giao diện cho người thiết kế | `uiux-feature-brief.md` |
| Kế hoạch gốc và thang điểm | `docs/ke-hoach-nhom.md` |
