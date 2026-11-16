# ADR-006: Mô hình dự báo 0–60′ chỉ để tham khảo, chỉ hiện khi vượt cổng chất lượng

- Trạng thái: Đã chấp nhận (2026-11-16)

## Bối cảnh
- Quy tắc Mức 1–4 là đáp án chấm KPI. Đổi thứ tự ưu tiên sẽ làm hỏng phép đo KPI.
- Quyết định khai thác chuyến bay nằm ngoài phạm vi học thuật.
- Số nhãn có được (A2, 30 ngày METAR): 383 giờ-sân bay dương tính, chiếm 18,0% số giờ có quan trắc. Phân bố lệch về VVDN (185 giờ, so với VVNB 113 và VVTS 85).

## Quyết định
1. Nhãn `y=1`: có METAR với `observed_at_utc ∈ (t, t+60′]` ghi mưa hoặc dông tại trạm, hoặc `visibility_m < 5000`. Nói cách khác, mô hình dự báo sân bay có lên Mức 1 trong 60 phút tới hay không.
2. Mọi đặc trưng phải thỏa `available_at ≤ t`. Dự báo Open-Meteo bị thiếu thì đánh dấu là thiếu (cờ `forecast_missing`), không lấp bằng Historical Forecast API.
3. **Cổng chất lượng**:
   - Mô hình đã hiệu chỉnh phải đạt CSI cao hơn cả 3 baseline (persistence, chỉ Open-Meteo, bộ quy tắc hiện tại) trên tuần cuối được giữ riêng.
   - Hai tập chia theo thời gian (walk-forward), có khoảng đệm.
   - Ngưỡng quyết định được chọn trên tập validation.
4. Dashboard chỉ hiện cột "Nguy cơ 0–60′ (chỉ tham khảo)" khi `gate_passed`. Thứ tự Mức 1–4 không bao giờ thay đổi.

## Hệ quả
- Nếu mô hình không thắng baseline, báo cáo ghi rõ kết quả âm tính và dashboard ẩn cột.
- Brier và reliability chỉ áp dụng cho xác suất của mô hình. Dự báo Open-Meteo là tất định có ngưỡng nên được chấm bằng POD, FAR, CSI (xem `src/analytics/verification.py`).
