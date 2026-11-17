# Bàn giao dự án MIS5007: Hỗ trợ điều phối viên rà soát thời tiết 3 sân bay

Dành cho: cả nhóm (A, B, C). Cập nhật: 2026-11-17.
Mọi con số trong tài liệu này lấy từ lần chạy ngày 2026-11-16/17 trên máy nhóm. Bảng số liệu đầy đủ nằm trong thư mục `ket-qua/` (mở bằng Excel). Chỗ nào chưa đo hoặc chưa kiểm chứng đều ghi rõ.

Các tệp đi kèm:

| Tệp | Dùng để |
|---|---|
| `he-thong-hien-tai.md` | **Đọc đầu tiên:** giải thích toàn bộ hệ thống (luồng dữ liệu, quy tắc, dashboard, cách chạy, thuật ngữ) |
| `checklist-cong-viec.xlsx` | Việc của từng người, trạng thái, người review, hạn (39 việc) |
| `ket-qua/ket-qua-3-moc-phat-lai.csv` | Kết quả xếp ưu tiên 3 mốc phát lại (dùng cho demo) |
| `ket-qua/kiem-dinh-quy-tac.csv` | Kiểm định dự báo mưa và thứ tự Mức 1–4 |
| `ket-qua/mo-hinh-nowcast-tap-kiem-tra.csv` | Mô hình dự đoán 0–60′ so với các cách đơn giản |
| `ket-qua/thoi-gian-chay-pipeline.csv` | Thời gian từng giai đoạn của 5 lần chạy pipeline |
| `../architecture/` | Kiến trúc, đánh giá, ma trận lưu trữ, từ điển dữ liệu, ADR |
| `uiux-feature-brief.md` | Mô tả dashboard cho người thiết kế giao diện |

## 1. Sản phẩm trong một câu

Hệ thống gom 3 nguồn dữ liệu (METAR, dự báo Open-Meteo, ảnh vệ tinh Himawari-9 kênh B13) rồi tự xếp sân bay nào cần rà soát trước (Đà Nẵng VVDN, Nội Bài VVNB, Tân Sơn Nhất VVTS), kèm lý do và bằng chứng.

| Mức | Ý nghĩa |
|---|---|
| 1 | Có mưa hoặc dông tại trạm, hoặc tầm nhìn dưới 5 km |
| 2 | Cần xác minh dữ liệu: bản tin quá 60 phút hoặc thiếu |
| 3 | Có dự báo mưa giờ tới (từ 0,1 mm) |
| 4 | Còn lại |

Thứ tự Mức 1–4 là đáp án chấm KPI. Mô hình dự đoán chỉ để tham khảo, không đổi thứ tự.

## 2. Tình trạng tổng quát

| Hạng mục | Tình trạng | Bằng chứng |
|---|---|---|
| Pipeline Spark (đọc, kiểm tra, ghép, xếp ưu tiên, ghi Parquet) | Chạy được | Lần chạy 2026-11-17: 414 dòng quyết định, 138 mốc, tổng 55,6 s |
| Đối soát số dòng | Khớp | Đọc = hợp lệ + loại: METAR 5328, Open-Meteo 6009, Himawari 66; loại 0 dòng |
| Dashboard 4 tab | Có | Quyết định, Đo KPI, Đối soát dữ liệu, Sức khỏe hệ thống. Bộ test dashboard 112/112 đạt |
| Kiểm thử tự động | 332/333 đạt | Test còn lại (`test_verification`) lỗi kết nối Spark khi chạy đủ, đạt 9/9 khi chạy riêng. Chưa tìm nguyên nhân gốc, nghi lỗi môi trường |
| Tài liệu kiến trúc, từ điển, 6 ADR | Có | `docs/architecture/` |
| Bộ mẫu nộp | Có | `dist/sample_dataset.zip`, 98 tệp, 2,9 MB nén |
| Đo KPI với người ngoài nhóm | **Chưa làm** | Công cụ đã có, chưa có số đo |
| Báo cáo, slide, video dự phòng, nhật ký, khai báo AI | **Chưa làm** | |

## 3. Kết quả đã chạy

### 3.1 Xếp ưu tiên trên 3 mốc phát lại (dùng cho demo)

| Mốc (UTC) | VVDN | VVNB | VVTS | Ghi chú |
|---|---|---|---|---|
| 2026-11-09 07:00 | Mức 1 | Mức 4 | Mức 4 | Một đáp án đúng: VVDN |
| 2026-11-09 08:00 | Mức 1 | Mức 1 | Mức 4 | **Đồng mức**: chấp nhận VVDN hoặc VVNB nếu nêu đúng lý do và giờ METAR |
| 2026-11-09 11:00 | Mức 1 | Mức 4 | Mức 3 | VVTS có dự báo mưa 0,1 mm; VVDN dự báo 6,2 mm |

Ở mốc 07:00 và 08:00, cột dự báo là **"khuyết"** (không có dự báo tự thu trước mốc). Đây là đúng theo thiết kế: dữ liệu tải bù không có dự báo và không được lấp bằng dữ liệu lịch sử.

Toàn bảng quyết định (414 dòng): Mức 1 = 128, Mức 2 = 0, Mức 3 = 32, Mức 4 = 254. Chưa có dòng Mức 2 nào trong dữ liệu hiện có, nên khi demo ca "bản tin cũ 90 phút" phải dùng ca mô phỏng có ghi nhãn.

### 3.2 Chất lượng dữ liệu

| Chỉ số | Giá trị |
|---|---|
| Dòng trùng hoàn toàn trong Bronze | METAR 57%, Open-Meteo 76% (do cửa sổ tải lùi 2 giờ) |
| Phân đoạn thực sự thiếu trên S3 | 6 (2026-11-15 14:40Z; 2026-11-16 02:40Z, 04:40Z, ...) |
| Máy thu ngừng chạy | 11-11 đến 11-15 (phân loại `RECEIVER_FAULT`) |
| `inner_bt_min_c` rỗng | 2/45 dòng Himawari |
| Phân loại sai nguyên nhân thiếu | Có test với khoảng thiếu nhân tạo |
| Ảnh thay thế khi thiếu mốc | Đã sửa lỗi gắn sai thời điểm, nay giữ đúng `source_slot_utc` |

### 3.3 Kiểm định dự báo mưa (bảng 2×2 tại ngưỡng 0,1 mm, toàn bộ sân bay)

Mẫu: 258 giờ-sân bay, 105 giờ có mưa, cửa sổ 2026-11-09 10:00 đến 2026-11-12 23:00.

| Ngưỡng | Bắt đúng | Bỏ sót | Báo nhầm | Bắt trúng (POD) | Báo nhầm (FAR) | CSI |
|---|---|---|---|---|---|---|
| 0,1 mm | 87 | 18 | 42 | 0,829 | 0,326 | 0,592 |
| 0,5 mm | 74 | 31 | 18 | 0,705 | 0,196 | 0,602 |
| 1,0 mm | 46 | 59 | 10 | 0,438 | 0,179 | 0,400 |

Theo sân bay (0,1 mm): VVDN CSI 0,860 (74/86 giờ có mưa); VVNB CSI 0,320 (21/86); VVTS CSI 0,139 (10/86, báo nhầm 26 lần). Dự báo tốt ở Đà Nẵng, yếu ở Tân Sơn Nhất. **Lưu ý khi báo cáo:** VVDN có 86% giờ có mưa nên điểm cao một phần do tỷ lệ mưa nền cao; tháng 11 là mùa khô ở miền Bắc và miền Nam.

### 3.4 Thứ tự Mức 1–4 có khớp thực tế không

Tỷ lệ có mưa/dông trong 60 phút sau, theo mức:

| Mức | Số dòng | Tỷ lệ có sự kiện | Khoảng tin cậy 95% |
|---|---|---|---|
| 1 | 128 | 98,4% | 94,5% đến 99,6% |
| 3 | 28 | 35,7% | 20,7% đến 54,2% |
| 4 | 231 | 4,3% | 2,4% đến 7,8% |

Kết luận: **Mức 1 ≥ Mức 3 ≥ Mức 4 đúng** (kiểm tra đơn điệu đạt). Mức 2 không có dòng nào nên chưa đánh giá. Khoảng tin cậy lạc quan hơn thực tế vì các dòng cách nhau 5 phút, cửa sổ 60 phút chồng lên nhau.

### 3.5 Mô hình dự đoán 0–60′ (chỉ để tham khảo)

Chia theo thời gian: huấn luyện 2.502 dòng (338 dương tính), kiểm chứng 714 (108), kiểm tra tuần cuối 1.008 (301). Chọn GBT (CSI kiểm chứng 0,377, hơn LR 0,335).

| Cách dự đoán (tập kiểm tra) | CSI | POD | FAR |
|---|---|---|---|
| Giữ nguyên trạng thái giờ trước (persistence) | **0,702** | 0,767 | 0,108 |
| Mô hình GBT | 0,683 | 0,850 | 0,224 |
| Luật hiện tại (Mức 1 hoặc 3) | 0,653 | | |
| Mô hình LR | 0,649 | 0,831 | 0,251 |
| Chỉ dùng Open-Meteo | 0,467 | | |

**Không qua cổng chất lượng**: CSI 0,683 không vượt persistence 0,702, nên theo ADR-006 mô hình **không được hiển thị** trên dashboard như một khuyến nghị. Báo cáo cần nêu đây là kết quả âm tính. Số nhãn: 383 giờ-sân bay dương tính trên 2.132 giờ có quan trắc (18%): VVDN 185, VVNB 113, VVTS 85.

### 3.6 Hiệu năng và dung lượng (đã đo)

| Đo | Kết quả |
|---|---|
| Pipeline Spark, 134 mốc, chạy lạnh | 228 s trước sửa, **52 s** sau sửa |
| Pipeline Spark, chạy ấm | 218 s trước, **18 s** sau |
| Nguyên nhân chậm cũ | Dựng spine bằng Python RDD, mỗi stage khởi động lại Python worker. Giả thuyết "JVM khởi động là nút thắt" **bị bác bỏ** (lập kế hoạch truy vấn chỉ 0,5 s) |
| Giai đoạn lần chạy 2026-11-17 | Khởi tạo 16,8 s · đọc 8,3 s · join + ưu tiên 8,5 s · ghi 14,3 s |
| Satpy, 6 mốc | Tuần tự 130 s; Spark p=2 89 s; p=4 **53 s** (nhanh 2,44×) |
| Một mốc Himawari (lạnh) | 19,1 s, RAM đỉnh khoảng 410 MB |
| Himawari tải về | **5,67 MB mỗi lượt** (kế hoạch ước 2,5 MB), khoảng 816 MB/ngày |
| Ngoại suy 30 ngày Himawari | 24,5 GB, khoảng 26 giờ tuần tự (khoảng 10,7 giờ ở p=4) |
| Lake Silver + Gold mỗi phiên bản | khoảng 1,5 MB |

### 3.7 Ma trận lưu trữ

Kho tệp cục bộ + Parquet có phiên bản là lớp chính. HDFS giữ vai trò minh họa xử lý phân tán và ghi lake từ Spark, không nằm trên đường găng của demo. PostgreSQL chỉ so sánh định tính. Bảng đầy đủ: `docs/architecture/02-assessment.md` mục 4. **Chưa đo được** thông lượng WebHDFS theo số tệp (mục B6): mới có một số 0,86 MB/s cho 49 tệp, 16,6 MB, chưa tách được nguyên nhân.

## 4. Vấn đề đã tìm ra và đã sửa

| Vấn đề | Mức | Kết quả |
|---|---|---|
| Pipeline chậm 3,5 phút | Cao | Còn 52 s lạnh, 18 s ấm |
| Không có Parquet, không ghi được lên HDFS | Cao | Đã sửa, ghi Parquet vào `data/` hoặc `hdfs://` |
| Dashboard tự tính mức ưu tiên (hai đường tính) | Cao | Chỉ còn Spark tính; dashboard chỉ đọc Gold |
| Dự báo lấy về muộn lọt vào kết quả phát lại (rò rỉ tương lai) | Cao | Đã sửa, có test hồi quy |
| Mất dòng khi mọi METAR đều sau mốc | Cao | Đã sửa, luôn đủ 3 dòng mỗi mốc |
| Tệp thô `.DAT.bz2` không bao giờ bị xóa | Cao | Đã sửa; daemon giữ 2 giờ |
| Không ai thấy khi máy thu ngừng nhiều ngày | Cao | Có tab Sức khỏe hệ thống |
| Test đọc dữ liệu thật trỏ vào thư mục rỗng | Trung bình | Sửa trong `tests/spark/real_data.py` |
| Thứ tự mốc ngược làm "Xem mốc mới" nhảy sai | Trung bình | Sửa trong `src/dashboard/app.py` |

## 5. Rủi ro và vấn đề còn mở

1. **Đĩa gần đầy.** Lúc đo, ổ E: còn khoảng 226 MB trống và bộ nhớ gần cạn. Tải bù Himawari phải chạy trên ổ D:. Trước khi demo hoặc bật máy thu, kiểm tra dung lượng.
2. **Số đo sức khỏe hệ thống chưa dùng được.** Độ phủ quét Himawari 24 giờ gần nhất là 1/141 (0,7%), 23 giờ thiếu, do máy thu không chạy. Cần bật `run_daemon.bat` chạy liên tục 24 giờ rồi đo lại trước khi đưa vào báo cáo.
3. **Chưa xin giảng viên chấp thuận.** Kế hoạch nhóm quy định thay đổi lớn sau buổi 6 cần giảng viên đồng ý (lớp phân tích, mô hình, tải bù). Không thấy bằng chứng đã xin. Nếu không được duyệt, phần dự đoán rút khỏi demo.
4. **Chưa có ngày bảo vệ chính xác**, nên chưa chốt được hạn.
5. **Mức 2 chưa có ca thật** trong dữ liệu; kịch bản demo "bản tin cũ" phải ghi nhãn là ca mô phỏng.
6. **Spark trên Windows hay mất kết nối Python worker** khi chạy cả bộ test. Chạy trước và chụp kết quả; đừng chạy trực tiếp trên sân khấu.
7. **Mới 1 lần đo HDFS** (0,86 MB/s). Chưa kết luận được "tệp nhỏ là nguyên nhân".
8. **Giả thuyết đã nêu nhưng chưa đối chiếu lại tài liệu Open-Meteo chính thức:** lượng mưa là tổng của giờ liền trước. Từ điển đã ghi theo giả thuyết này; người review (C) cần đối chiếu.

## 6. Việc của từng người

Chi tiết, trạng thái, người review, hạn: `checklist-cong-viec.xlsx`.

| Người | Việc chặn chính |
|---|---|
| A (Điều phối & nghiệp vụ) | Xin giảng viên chấp thuận; mời người ngoài nhóm đo KPI (hai cách xem, có ca đồng mức); phân tích giá trị và chi phí; báo cáo ≤ 30 trang; nhật ký, khai báo AI |
| B (Kiến trúc & dữ liệu) | Đo 24 giờ cho ma trận lưu trữ; ghi điều kiện sử dụng từng nguồn; chạy lại hệ thống trên máy mình để nghiệm thu chéo; 2 slide kiến trúc và ma trận |
| C (Xử lý dữ liệu) | Hỗ trợ B chạy lại; đối chiếu nghĩa trường Open-Meteo; giữ bộ mẫu nộp; giải thích được một bước Spark |
| A và C | Diễn giải kết quả (kể cả kết quả âm tính); slide ≤ 10; video hoặc ảnh dự phòng ≤ 3 phút; diễn tập demo |
| Cả nhóm | Tập vấn đáp cá nhân; review chéo; chốt đủ 5 sản phẩm |

## 7. Cách xem và chạy lại

**Người không cần mã:** nhấp đúp `run_dashboard.bat`, mở `http://localhost:8501`. Chưa xác nhận bước này trên máy từng người.

**Người nghiệm thu (B):** trong thư mục dự án.

```bash
.venv\Scripts\python.exe -m src.spark.run_pipeline --storage-root data      # khoảng 1 phút
.venv\Scripts\streamlit.exe run src/dashboard/app.py                        # dashboard
.venv\Scripts\pytest.exe -q                                                 # 10–16 phút
.venv\Scripts\python.exe scripts/package_sample.py --storage-root data --out dist/sample_dataset.zip
```

Kiểm tra đúng: mỗi mốc có đúng 3 dòng sân bay; chạy lại cùng đầu vào cho cùng số khóa; đối soát `is_reconciled: true`.

## 8. Năm sản phẩm phải nộp và thang điểm

| # | Sản phẩm | Giới hạn | Tình trạng |
|---|---|---|---|
| 1 | Báo cáo toàn văn | ≤ 30 trang | Chưa làm |
| 2 | Bộ dữ liệu | Mẫu, từ điển, nguồn, điều kiện sử dụng | Có |
| 3 | Bản thử nghiệm | Mã, cấu hình, kết quả test, hướng dẫn | Có |
| 4 | Slide bảo vệ | ≤ 10 slide | Chưa làm |
| 5 | Minh chứng demo | Trực tiếp + video/ảnh dự phòng ≤ 3 phút | Chưa làm |

Thang điểm: Spark và tái lập 25% · kiến trúc và lưu trữ 20% · vấn đề/KPI 15% · dữ liệu và chất lượng 15% · trình bày và demo 15% · kết quả, kiểm thử, đối soát 10%.

Gợi ý 10 slide: vấn đề · quyết định/KPI · dữ liệu (5V) · kiến trúc · ma trận lưu trữ · 2 slide xử lý và chất lượng · kết quả · demo · chi phí/rủi ro/hướng phát triển. Kịch bản video: 30 giây quyết định, 60 giây chạy Spark và chỉ bảng lỗi, 60 giây đọc kết quả và truy nguyên, 30 giây đối soát và chốt KPI.

## 9. Câu hỏi hay gặp khi vấn đáp

| Câu hỏi | Trả lời ngắn |
|---|---|
| Vì sao không Kafka/Airflow? | Dữ liệu MB, một máy; có ngưỡng xem xét lại (ADR-005) |
| Vì sao Parquet thay cơ sở dữ liệu? | Nhanh nhất trên một máy, có phiên bản, làm bản dự phòng demo (ADR-001, ma trận) |
| HDFS để làm gì? | Minh họa phân tán và ghi lake từ Spark; ngoài đường găng demo (ADR-002) |
| Dự báo thiếu thì sao? | Hiển thị "dự báo khuyết", không lấp |
| Sao mô hình không dùng? | Không thắng persistence trên tuần giữ riêng; quy tắc loại đặt từ trước (ADR-006) |
| Dữ liệu nhỏ sao gọi Big Data? | Dựa vào đa dạng nguồn, tốc độ thu, độ tin cậy, khả năng mở rộng, không dựa dung lượng |
| Tại sao Spark chậm trước đây? | Python RDD khởi động worker mỗi stage, không phải JVM; sửa còn 52 s |

## 10. Lưu ý trước khi nộp

- Bộ mẫu chỉ chứa ngày 2026-11-09 (cửa sổ phát lại) và ngày chạy, không chứa dữ liệu tải bù hay khóa truy cập. Đừng thêm tệp lạ vào gói.
- Các thay đổi hôm nay (file Excel, tài liệu bàn giao, thư mục `ket-qua/`, dữ liệu chạy lại) chưa được lưu vào git. Báo người giữ kho mã trước khi nộp.
- Người hỏi theo mảng: nghiệp vụ, KPI, báo cáo hỏi A; nguồn, kiến trúc, lưu trữ hỏi B; chạy pipeline, lỗi, bộ mẫu hỏi C.
