# Tài liệu bàn giao UI/UX: Dashboard Hỗ trợ Điều phối viên Thời tiết Sân bay

> Nguồn: `src/dashboard/app.py` (Streamlit, chạy ở bản hiện tại), `src/spark/priority_rules.py`, `src/dashboard/kpi_evaluator.py`, spec Mô-đun 4.
> Mục tiêu tài liệu: designer hiểu sản phẩm làm gì, ai dùng, có những màn hình, trạng thái và dữ liệu nào, để thiết kế lại giao diện. Không cần đọc code.

---

## 1. Sản phẩm trong một câu

Công cụ giúp **điều phối viên** nhìn 3 sân bay (**Đà Nẵng VVDN, Nội Bài VVNB, Tân Sơn Nhất VVTS**) tại một mốc giờ và biết ngay **sân bay nào cần rà soát thời tiết trước**, kèm bằng chứng từ 3 nguồn dữ liệu:

| Nguồn | Nội dung | Dạng hiển thị |
|---|---|---|
| **METAR** (AviationWeather) | Bản tin quan trắc thực tế tại trạm | Chuỗi mã thô, ví dụ `VVDN 090700Z 36005KT 3000 RA ...` |
| **Open-Meteo** | Dự báo lượng mưa giờ tới | Con số mm/h |
| **Vệ tinh Himawari-9 kênh B13** | Ảnh hồng ngoại mây quanh sân bay | Ảnh PNG, chuỗi tối đa 6 khung, 10 phút/khung |

## 2. Người dùng

| Vai trò | Nhu cầu chính |
|---|---|
| **Điều phối viên** (người dùng chính) | Biết nhanh sân bay ưu tiên số 1 và lý do; kiểm tra bằng chứng gốc nếu nghi ngờ |
| **Người thử nghiệm KPI** (người ngoài nhóm) | Làm bài thử: chọn sân bay, lý do và giờ bản tin trong thời gian bấm giờ |
| **Giảng viên / người chấm** | Xem kết quả KPI, kiểm chứng chất lượng dữ liệu, chạy lại pipeline |

## 3. Kiến trúc thông tin

```
App
├── Sidebar (luôn hiển thị)
│   ├── Chế độ vận hành: LIVE / Phát lại
│   ├── Chọn mốc thời gian
│   ├── [LIVE] Cập nhật dữ liệu realtime
│   ├── Thứ tự hiển thị bảng
│   └── Vận hành pipeline PySpark
├── Header: tiêu đề + dòng trạng thái
└── 3 Tab
    ├── Tab 1: Rà soát Quyết định & Truy nguyên Bằng chứng   ← màn hình chính
    ├── Tab 2: Đo lường & Chấm điểm KPI
    └── Tab 3: Đối soát Dữ liệu, Quarantine & Lưu trữ           ← màn hình kỹ thuật
```

---

## 4. Logic nghiệp vụ designer cần nắm

### 4.1. Bốn mức ưu tiên (mức 1 = khẩn nhất; xét từ trên xuống, khớp mức nào thì dừng ở mức đó)

| Mức | Nhãn | Điều kiện | Ví dụ lý do hiển thị |
|---|---|---|---|
| **1** | Nguy hiểm tại trạm | METAR còn mới (≤ 60 phút) **và** có mưa/dông tại trạm **hoặc** tầm nhìn < 5 km | "METAR mới (07:00Z) ghi nhận mưa/dông tại trạm (RA), tầm nhìn 3000m" |
| **2** | Cần xác minh dữ liệu | Không có METAR, hoặc METAR cũ hơn 60 phút | "Cần xác minh dữ liệu: Bản tin METAR đã cũ 95 phút (> 60 phút)" |
| **3** | Cảnh báo mưa dự báo | Trạm ổn nhưng dự báo mưa giờ tới ≥ 0.1 mm | "Điều kiện tại trạm tốt, nhưng dự báo mưa giờ tới đạt 1.2 mm (hiệu lực 09:00Z)" |
| **4** | Bình thường | Còn lại | "Điều kiện tại trạm bình thường, không có cảnh báo mưa giờ tới" |

→ Cần **hệ màu/biểu tượng cho 4 mức** dùng thống nhất toàn app.

### 4.2. Đồng mức ưu tiên đầu (Co-Priority Tie)
Nhiều sân bay có thể cùng mức cao nhất (ví dụ lúc 08:00Z cả VVDN và VVNB đều mức 1). Khi đó **chọn sân bay nào trong nhóm cũng đúng**. Giao diện phải thể hiện rõ đây là "nhóm đáp án tương đương", không phải sân bay này đứng trên sân bay kia.

### 4.3. Hai múi giờ
Mọi mốc giờ đều hiển thị song song **UTC** (`07:00Z`) và **giờ Việt Nam** (`14:00 ICT`, UTC+7). Người dùng có thể đọc hoặc nhập theo cả hai.

### 4.4. Hai chế độ vận hành
- **LIVE**: dữ liệu thật vừa thu thập; mốc mới nhất có nhãn `LIVE MỚI NHẤT`.
- **Phát lại lịch sử (Replay)**: mốc cố định (07:00Z, 08:00Z ngày 09/11) dùng cho bài thử KPI. Chuyển mốc mất dưới 1 giây và chạy được khi không có mạng.

---

## 5. Chi tiết tính năng theo màn hình

### 5.1. Sidebar

| # | Thành phần | Loại control | Hành vi / trạng thái |
|---|---|---|---|
| S1 | Logo + tên hệ thống + mô tả ngắn | Text | |
| S2 | **Chế độ vận hành** | Radio 2 lựa chọn | LIVE (mặc định) / Phát lại |
| S3 | **Chọn mốc thời gian** | Dropdown | Nhãn: `07:00Z (14:00 ICT) — 2026-11-09`, mốc mới nhất có badge LIVE. LIVE mặc định chọn mốc mới nhất, Replay mặc định chọn mốc đầu |
| S4 | [Chỉ LIVE] Tải kèm ảnh vệ tinh | Checkbox (mặc định bật) | Bật thì cập nhật lâu hơn (~15–25 giây) |
| S5 | [Chỉ LIVE] **Cập nhật dữ liệu realtime** | Nút primary | Hiện khối tiến trình → thành công (hiện mốc mới, tải lại trang) / lỗi (hiện chi tiết) |
| S6 | **Thứ tự hiển thị bảng** | Dropdown | Theo mức ưu tiên (mặc định) / Mã A→Z / Mã Z→A. Dùng để chứng minh đổi thứ tự dòng không đổi đáp án |
| S7 | Bơm dữ liệu lỗi kiểm thử | Checkbox | Chỉ dùng khi demo/kiểm thử |
| S8 | **Chạy lại pipeline PySpark** | Nút phụ | Tiến trình → thành công / lỗi (tối đa 3 phút) |
| S9 | Cảnh báo đồng bộ thất bại / không có dữ liệu | Alert vàng/đỏ | Khi lỗi đọc dữ liệu |

### 5.2. Header
- Tiêu đề hệ thống.
- **Dòng trạng thái**: chế độ (LIVE/REPLAY) · mốc đang chọn (UTC + ICT + ngày) · thứ tự bảng đang dùng.

### 5.3. Tab 1: Rà soát Quyết định & Truy nguyên Bằng chứng (màn hình chính)

**A. Banner quyết định** (phần quan trọng nhất của màn hình)
- Một sân bay ưu tiên: banner thông tin "Sân bay cần rà soát đầu tiên: **VVDN** — <lý do>".
- Đồng mức: banner cảnh báo "ĐỒNG MỨC ƯU TIÊN ĐẦU — chọn bất kỳ: **VVDN, VVNB**".

**B. Bảng tổng hợp 3 sân bay** (luôn đúng 3 dòng)

| Cột | Ví dụ |
|---|---|
| Mức ưu tiên | Mức 1 |
| Mã sân bay | VVDN |
| Tên sân bay | Đà Nẵng (VVDN) |
| Lý do phân loại | câu tiếng Việt như mục 4.1 |
| Giờ quan trắc | 07:00Z (14:00 ICT) |
| Tuổi METAR | 12 phút / N/A |
| Tầm nhìn | 3000m, hoặc `9999m (≥9999)` |
| Mưa giờ tới (dự báo) | 1.2 mm/h |
| Mây B13 (vệ tinh) | "Mây đối lưu mạnh (BT min: -65.3°C)" |
| Trạng thái nguồn | Đủ 3 nguồn / Khuyết nguồn |

**C. Truy nguyên bằng chứng (drill-down)**: chọn 1 sân bay → layout 2 cột:

*Cột trái*
1. **METAR gốc**: khối code hiển thị chuỗi thô; các chỉ số: giờ quan trắc, tuổi dữ liệu, tầm nhìn, giờ hệ thống nhận tin; mã thời tiết tại trạm / lân cận / xu hướng (TEMPO, BECMG); số hiệu bản sửa đổi / SPECI.
2. **Dự báo Open-Meteo**: lượng mưa giờ tới (mm/h) + thời điểm nhận dự báo. Nếu mốc lịch sử không có dự báo thì hiện ghi chú "mặc định 0.0 mm/h theo nguyên tắc bảo thủ".

*Cột phải*
3. **Ảnh vệ tinh Himawari-9 B13**
   - Thanh trượt thời gian chọn 1 trong tối đa 6 khung (mặc định khung mới nhất) → ảnh lớn + chú thích giờ.
   - Fallback: không có chuỗi → ảnh mới nhất → không có ảnh → thông báo "Chưa có ảnh".
   - 4 chỉ số: Độ phủ hợp lệ (%), Nhiệt độ sáng cực tiểu BT min (°C), Tỷ lệ mây lạnh < -40°C (%), Tỷ lệ đối lưu mạnh < -60°C (%).
   - **Dòng ghi công bắt buộc** (yêu cầu bản quyền, không được bỏ): `Processed Product | Source: JMA/NOAA Himawari-9 B13 via AWS Open Data | Cropped & Annotated by Group`.

### 5.4. Tab 2: Đo lường & Chấm điểm KPI

Mục đích: đo xem dashboard có giúp người dùng quyết định **nhanh hơn ít nhất 30%** so với tự đọc 3 nguồn rời hay không.

**A. Công tắc chế độ thử nghiệm** (radio ngang)
- **Baseline: 3 nguồn rời**: ẩn mọi gợi ý ưu tiên. Ba cột: (1) ô text METAR thô của 3 sân bay, chỉ đọc; (2) bảng mưa dự báo thô; (3) dropdown chọn sân bay + 1 ảnh vệ tinh.
- **Dashboard tổng hợp**: bảng rút gọn có mức ưu tiên, lý do, giờ quan trắc, mưa, BT min.
- ⚠️ Ở chế độ Baseline, phải bảo đảm không có gì trên màn hình làm lộ đáp án (kể cả banner ở Tab 1 và màu theo mức).

**B. Bộ bấm giờ**
- Nút: **Bắt đầu nhận ca** / **Dừng & Ghi nhận** / **Đặt lại**.
- Đồng hồ hiển thị số giây + trạng thái `idle | running | stopped`.
- Lưu ý: đồng hồ hiện không tự chạy trên màn hình mà chỉ cập nhật khi có thao tác. Nên thiết kế đồng hồ đếm trực quan.

**C. Phiếu nộp kết quả**
| Trường | Control |
|---|---|
| Mã người thử | Text, mặc định `DISPATCHER_01` |
| Thứ tự lượt thử | Dropdown 1 / 2 |
| Sân bay cần rà soát đầu tiên | Dropdown VVDN / VVNB / VVTS |
| Lý do căn cứ | Dropdown 5 lựa chọn: Mưa/dông tại trạm · Tầm nhìn thấp (< 5000m) · METAR thiếu/quá cũ (> 60 phút) · Dự báo mưa giờ tới (≥ 0.1 mm/h) · Bình thường |
| Giờ bản tin làm căn cứ | Text tự do, nhận `07:00Z`, `0700Z`, `090700Z`, `14:00`, `14h00` |
| **Nộp & chấm điểm** | Nút |

Quy tắc: phải bấm "Bắt đầu" trước khi nộp (nếu chưa thì hiện cảnh báo). Nộp xong đồng hồ tự dừng; lượt sau phải bấm "Bắt đầu" lại.

**D. Kết quả chấm**
- **ĐẠT**: banner xanh kèm số giây.
- **CHƯA ĐẠT**: banner đỏ kèm lỗi cụ thể (sai sân bay / sai lý do / sai giờ, có thể sai nhiều mục cùng lúc), và hộp "Đáp án đúng": sân bay chấp nhận, giờ chuẩn (UTC + ICT), lý do chuẩn.

**E. Thống kê KPI tổng hợp**
- Bảng so sánh Baseline và Dashboard: tổng lượt thử · lượt đạt · tỷ lệ đạt % · thời gian trung vị của lượt đạt (giây).
- Chỉ số lớn: **% rút ngắn thời gian** + chênh lệch so với ngưỡng 30%.
- Badge: "ĐẠT MỤC TIÊU ≥ 30%" / "Chưa đạt ngưỡng 30%".
- Gợi ý: phần này hợp để làm biểu đồ (cột so sánh thời gian trung vị, tỷ lệ đạt).

### 5.5. Tab 3: Đối soát Dữ liệu, Quarantine & Lưu trữ (dành cho giảng viên / kỹ thuật)

1. **Bảng bảo toàn dòng**: mỗi nguồn (metar, open_meteo, himawari_metrics) có các cột: dòng đọc · cách ly · hợp lệ trước lọc trùng · giữ lại · trùng bị loại · trạng thái ("BẢO TOÀN 100%" / "LỆCH DÒNG"). Có banner tổng nếu tất cả khớp. Đẳng thức minh họa: `đọc = hợp lệ + cách ly`, `hợp lệ = giữ lại + trùng`. Có thể trực quan hóa dạng luồng/Sankey.
2. **Nhật ký bản ghi cách ly (Quarantine)**: bảng các bản ghi lỗi (mã lỗi, file nguồn, payload thô, có phải dữ liệu giả lập kiểm thử không). Rỗng thì hiện bản xem trước dữ liệu lỗi mẫu.
3. **Lỗi tệp nhị phân vệ tinh**: đường dẫn, dung lượng, mã lỗi, thông điệp, cờ giả lập.
4. **Thống kê 5V & HDFS**: 3 chỉ số (tổng log thu nạp, tổng dung lượng byte, tổng bản ghi) + bảng nhật ký đồng bộ Hadoop (thông lượng MB/s, độ trễ ms).

---

## 6. Danh sách trạng thái cần thiết kế

| Ngữ cảnh | Trạng thái |
|---|---|
| Toàn app | Không có dữ liệu; đồng bộ lỗi; đang tải |
| Cập nhật LIVE / chạy pipeline | Đang chạy (có thể 15 giây đến 3 phút); thành công; lỗi kèm chi tiết |
| Banner quyết định | 1 sân bay; đồng mức 2–3 sân bay |
| Bảng sân bay | Mức 1/2/3/4; khuyết nguồn; tầm nhìn ≥ 9999; METAR N/A |
| Drill-down dự báo | Có dự báo; không có (mốc lịch sử) |
| Ảnh vệ tinh | Chuỗi 6 khung; ít khung hơn; 1 ảnh; không có ảnh; file bị mất |
| Bấm giờ | idle; running; stopped |
| Nộp bài | Chưa bấm bắt đầu; ĐẠT; CHƯA ĐẠT (1 hoặc nhiều lỗi) |
| KPI | Chưa có lượt thử (N/A); đạt / chưa đạt mục tiêu 30% |
| Đối soát | Khớp hết; có nguồn lệch; chưa có báo cáo |

## 7. Ràng buộc & lưu ý cho designer

- **Nền tảng hiện tại là Streamlit**, nên khả năng tùy biến bị giới hạn (component chuẩn, CSS hạn chế). Nếu thiết kế vượt ra ngoài phạm vi này, cần thống nhất với dev trước. Màn hình mục tiêu là desktop, layout rộng.
- **Ngôn ngữ giao diện: tiếng Việt**; giữ nguyên các thuật ngữ chuyên ngành (METAR, TEMPO, BECMG, SPECI, B13, BT min, ICAO).
- Mỗi lựa chọn trong sidebar ảnh hưởng tới cả 3 tab.
- Ưu tiên thứ bậc thông tin ở Tab 1: **Banner quyết định → bảng 3 sân bay → bằng chứng chi tiết**. Điều phối viên phải trả lời được "sân bay nào, vì sao, giờ bản tin nào" trong vài giây.
- Không chỉ dựa vào màu để phân biệt mức ưu tiên (người mù màu, in đen trắng): dùng thêm nhãn/biểu tượng.
- Tab 3 và các nút vận hành pipeline mang tính kỹ thuật, có thể tách thành khu vực "Quản trị" riêng.

## 8. Điểm UX hiện tại nên cải thiện (gợi ý)

1. Tiêu đề tab và nhãn quá dài, trộn Anh–Việt, nhiều emoji → cần rút gọn, thống nhất.
2. Bảng tổng hợp chưa có màu/nhãn trực quan theo mức ưu tiên.
3. Đồng hồ KPI không chạy trực tiếp trên màn hình.
4. Chế độ Baseline vẫn có thể thấy đáp án nếu người thử chuyển sang Tab 1.
5. Sidebar trộn công cụ của điều phối viên (chọn mốc) với công cụ kỹ thuật (pipeline, bơm lỗi).
6. Ô nhập giờ là text tự do, không có gợi ý định dạng ngay trong ô (chỉ có placeholder).
7. Chuỗi METAR thô khó đọc → có thể giải mã/tô sáng từng phần (tầm nhìn, hiện tượng thời tiết).
8. Chưa có tổng quan trên bản đồ, dù đây là 3 sân bay ở 3 miền.
