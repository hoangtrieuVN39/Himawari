# Big data \- Kế hoạch dự án nhóm

# Hỗ trợ điều phối viên rà soát thời tiết tại ba sân bay Việt Nam



**Phạm vi đề xuất:** nhóm 3 thành viên, làm việc trong 3 tuần, chạy bản thử nghiệm trên máy cá nhân\. Theo dõi Đà Nẵng \(VVDN\), Nội Bài \(VVNB\) và Tân Sơn Nhất \(VVTS\)\. Đà Nẵng là nơi kiểm tra trọn quy trình đầu tiên\. Các buổi 1, 3, 6 và 8 là mốc trong hướng dẫn học phần\. Việc xếp các buổi đó vào ba tuần dưới đây là lịch làm việc đề xuất, có thể dịch theo thời khóa biểu thực tế\.



### 1\. Vấn đề, người sử dụng và quyết định quản lý — CLO4



**Tình huống nghiệp vụ giả định\.** Một nhóm điều phối dịch vụ mặt đất phụ trách theo dõi nhiều sân bay\. Đầu mỗi ca, điều phối viên phải mở riêng ảnh mây, bản tin tại trạm và dự báo, rồi ghi lại những nơi cần kiểm tra tiếp\. Giả thuyết của đồ án là một bảng tổng hợp có dấu thời gian và bằng chứng nguồn sẽ rút ngắn bước rà soát này\. Nhóm chưa có dữ liệu nội bộ về năng suất, chậm chuyến hay thiệt hại\.



**Một quyết định cần hỗ trợ:** trong lượt rà soát hiện tại, điều phối viên nên kiểm tra sân bay nào trước và bản tin nào giải thích lựa chọn đó? Nếu nhiều sân bay cùng thuộc mức cao nhất, điều phối viên có thể chọn bất kỳ sân bay nào trong nhóm ấy và dẫn căn cứ riêng của sân bay đã chọn\. Người dùng mở chi tiết để xác minh và ghi quyết định rà soát\. Quyết định khai thác chuyến bay nằm ngoài phạm vi học thuật\.



**Đầu ra phục vụ quyết định\.** Một bảng ba dòng gồm sân bay, mức ưu tiên rà soát \(có thể đồng mức\), lý do, giờ quan trắc gần nhất, tầm nhìn, dự báo mưa giờ tới, ảnh B13 gần nhất và tuổi của từng nguồn\. Bấm vào một dòng để xem bản tin gốc, thời điểm nhận dự báo và chuỗi ảnh trước sự kiện\. Có chế độ phát lại một ca có thật\.



|**Chỉ số**|**Cách đo và bằng chứng**|
|---|---|
|**KPI chính — thời gian chọn đúng sân bay cần rà soát đầu tiên và dẫn đúng căn cứ**|- Bắt đầu khi người thử nhận ca\.<br>- Dừng khi họ chọn một sân bay thuộc mức ưu tiên cao nhất, nêu đúng lý do tại sân bay đó và chỉ đúng giờ của bản tin làm căn cứ\. Lượt kết thúc với đáp án sai, thiếu căn cứ hoặc bỏ dở được ghi riêng\. Với mỗi cách xem, báo cáo thời gian trung vị của lượt đạt cùng số lượt đạt trên tổng số lượt thử\. Mức giảm 30% là mục tiêu đề xuất để đối chiếu sau khi đo\.|
|KPI bổ trợ|Tỷ lệ dòng có đủ bằng chứng và dữ liệu còn mới; tỷ lệ lượt quét đủ phân đoạn đã xử lý; độ trễ trung vị và P95 từ lúc nhận đủ đầu vào đến lúc công bố kết quả\. Công bố mẫu số, khoảng đo và giờ thiếu\.|
|Giá trị và chi phí|Ước tính chênh lệch thời gian cho một lần chọn sân bay đầu tiên từ thử nghiệm với người ngoài nhóm\. Tách chi phí máy, ổ đĩa, băng thông và công bảo trì\. Đo dung lượng tải, dung lượng giữ lại và thời gian chạy trong 24 giờ\. Chỉ quy đổi ra tiền khi có đơn giá và giả định sử dụng được ghi rõ\.|



**Cách tổ chức thử nghiệm ở tuần 3**



- Mời người ngoài nhóm chưa từng dùng dashboard\. Hướng dẫn mọi người cùng một cách đọc mã trạm, giờ và hiện tượng trong METAR\. Dùng một ca luyện tập riêng với phiếu quy tắc chung cho cả hai cách xem\.

- Mỗi người thực hiện cả hai cách: mở ba nguồn riêng và dùng dashboard\. Dùng các ca khác nhau có độ khó tương đương, gồm ca có một đáp án và ca đồng mức\. Đổi thứ tự hai cách giữa những người tham gia và luân phiên bộ ca để giảm ảnh hưởng của việc nhớ đáp án\. Cố định dữ liệu ở cùng mốc phát lại cho hai cách\.

- Trước buổi đo, người phụ trách lập đáp án cho từng ca từ bản tin gốc: tập sân bay được chấp nhận, lý do và giờ bản tin tương ứng của từng sân bay\. Người review kiểm tra độc lập\. Dùng giờ quan trắc của METAR làm căn cứ trong các ca chấm METAR, cho phép ghi UTC hoặc giờ Việt Nam quy đổi đúng\.

- Phiếu đo mỗi lượt ghi mã người thử, mã ca, cách xem, thứ tự thử, sân bay đã chọn, lý do, giờ bản tin, số giây và trạng thái đạt/chưa đạt/bỏ dở\. Báo cáo cỡ mẫu thực tế và diễn giải kết quả trong phạm vi người ngoài nhóm đã được hướng dẫn\.

    

### 2\. Dữ liệu, 5V và quản trị — CLO2/CLO4



Giới hạn bài nộp ở **3 nguồn**\. Dữ liệu dùng để phát lại được lấy từ ba nguồn này và đóng gói thành mẫu cục bộ\. Thu dự báo từ ngày 1 để giữ đúng phiên bản có trước từng ca\.



|Nguồn|Dữ liệu và nhịp thu đề xuất|Truy cập và nội dung cần ghi khi nộp|
|---|---|---|
|Himawari\-9, JMA/NOAA trên AWS|Kênh hồng ngoại B13, giờ quét, mã phân đoạn, thông tin định vị\. Ảnh toàn vùng có chu kỳ 10 phút\. Tải một lần các phân đoạn cần cho ba vùng rồi cắt vùng quanh sân bay\.|Kho công khai\. Ghi JMA/NOAA, ngày truy cập, đường dẫn tệp gốc và bước xử lý ảnh\. Ảnh nhóm đã cắt hoặc thêm chú giải cần ghi là sản phẩm đã xử lý\. [Điều kiện phân phối](https://registry.opendata.aws/noaa-himawari/)|
|METAR, Aviation Weather Center|Gọi chung ba mã sân bay mỗi 10 phút\. Giữ giờ quan trắc, giờ nhận, bản tin gốc, mã hiện tượng và tầm nhìn\. Thu bổ sung bản tin gần đây từ cùng API để chọn ca phát lại\.|Ghi nguồn phát hành, URL, ngày lấy và giới hạn gọi\. [Tài liệu API](https://aviationweather.gov/data/api/) · [Điều kiện sử dụng NWS](https://www.weather.gov/disclaimer)|
|Open\-Meteo|Lưu dự báo lượng mưa theo giờ và giờ hiệu lực cho từng tọa độ\. Gọi mỗi giờ một lần mỗi sân bay, giữ phiên bản cũ\.|Dùng gói miễn phí cho đồ án phi thương mại, ghi công nguồn theo điều khoản\. [Điều kiện sử dụng](https://open-meteo.com/en/terms) · [Hạn mức](https://open-meteo.com/en/pricing)|



**5V cho báo cáo tiến độ 1\.** *Volume:* 7 ngày × 144 lượt quét/ngày = 1\.008 lượt dự kiến, ghi riêng số tệp và byte thực tải\. *Velocity:* ảnh, quan trắc và dự báo đến khác thời điểm\. *Variety:* tệp ảnh phân đoạn, bản tin khí tượng và JSON dự báo\. *Veracity:* nguồn đến trễ, ảnh thiếu phân đoạn, bản tin sửa, đơn vị và múi giờ\. *Value:* rút ngắn thời gian chọn sân bay cần rà soát và cho phép kiểm tra lý do\.



**Tập dữ liệu khả thi\.** Nhóm bố trí một máy chạy liên tục ít nhất 7 ngày, cả ngày lẫn đêm\. Trong hai tuần đầu, chốt trước một cửa sổ đo đủ 168 giờ để đánh giá độ phủ, tối đa 3\.024 cặp sân bay–lượt quét cho ba sân bay\. Ghi thời điểm bắt đầu/kết thúc theo UTC\. Theo từng ngày, đối soát ba số: lượt quét dự kiến, lượt có đủ phân đoạn cần dùng trên nguồn và lượt xử lý thành công\. Gắn nguyên nhân cho khoảng thiếu, phân biệt sự cố máy thu với thiếu tệp trên nguồn\. Đánh dấu các lượt tải bù để đo độ trễ trực tiếp riêng\. Đóng gói thêm một cửa sổ 2–4 giờ để chạy lại nhanh, ưu tiên ca có mưa hoặc dông được METAR xác nhận\. Nếu cần dùng ca lịch sử chưa có bản dự báo đã lưu, hiển thị rõ phần dự báo khuyết\. Chấm KPI thời gian trên các ca có đầu vào tương đương\.



**Từ điển dữ liệu\.** Mỗi trường có tên, kiểu, ý nghĩa/đơn vị, nguồn, quy tắc chất lượng và mức nhạy cảm\. Ví dụ: `observed_at_utc` là timestamp quan trắc METAR; `visibility_m` là số mét kèm cờ giới hạn khi METAR ghi 9999; `rain_mm` gắn với đúng khoảng tích lũy của dự báo; `b13_valid_fraction` là tỷ lệ điểm ảnh dùng được trong vùng, thuộc \[0,1\]\. Lưu thời điểm sự kiện và thời điểm hệ thống nhận dữ liệu riêng\. Bộ mẫu chỉ chứa dữ liệu khí tượng công khai; loại thông tin tài khoản và khóa truy cập khỏi hồ sơ nộp\.



### 3\. Kiến trúc và ma trận lưu trữ — CLO2



**Luồng xử lý:** S3/API → Python thu nhận và lưu bản gốc → Satpy đọc B13, định vị và trích ảnh/chỉ số theo sân bay → PySpark đọc dữ liệu theo schema → kiểm tra và tách lỗi → chuẩn hóa, ghép và tổng hợp → bảng quyết định → Streamlit hiển thị và phát lại\.



**Phương án chính đề xuất:** một kho tệp trên đĩa cục bộ cho bản thử, chia thư mục theo lớp dữ liệu và ngày UTC\. Giữ bản gốc ở định dạng nguồn, bảng chuẩn hóa và bảng kết quả ở Parquet\. Spark là công cụ xử lý\. Streamlit đọc bảng kết quả\. Bản thử không cần thêm hệ lưu trữ bổ trợ\.

|**Tiêu chí**|**Kho tệp cục bộ \+ Parquet \(đề xuất\)**|**PostgreSQL \(phương án so sánh\)**|
|---|---|---|
|Loại dữ liệu và workload|Giữ ảnh và bản tin gốc cạnh các bảng phân vùng\. Thuận lợi khi Spark quét nhiều giờ/ngày để tái xử lý\.|Mạnh ở bảng có quan hệ, truy vấn có chỉ mục và cập nhật từng dòng\. Lưu ảnh lớn trong CSDL làm tăng gánh nặng sao lưu\.|
|Độ trễ|Tạo bảng kết quả nhỏ cho mỗi lượt cập nhật\. Đo thời gian ghi và mở bảng trong demo\.|Thuận lợi cho tra cứu theo sân bay–giờ và nhiều người truy cập đồng thời\.|
|Mở rộng|Xử lý song song theo ngày hoặc lượt quét\. Chuyển lớp tệp sang kho đối tượng khi nhiều máy cùng truy cập\. Đĩa cục bộ giới hạn bản thử\.|Có chỉ mục, phân vùng dữ liệu, cần thiết kế và vận hành CSDL khi tăng tải\.|
|Quản trị|Dùng danh mục tệp, phiên bản, quyền thư mục và kiểm tra ghi hoàn tất\. Nhóm tự xử lý chống trùng và tranh chấp ghi\.|Có giao dịch, phân quyền và ràng buộc khóa\. Cần vận hành dịch vụ và sao lưu\.|
|Chi phí và căn cứ chốt|Tận dụng máy nhóm\. Đo byte/ngày, RAM và thời gian đọc một ngày dữ liệu\.|Ước tính công cấu hình, đĩa và sao lưu\. Ghi rõ tiêu chí nào được đo, tiêu chí nào được đánh giá định tính\.|

Tại buổi 6, dùng số đo 24 giờ và kết quả demo để chốt ma trận\. Báo cáo giải thích lợi ích phân tán theo công việc cụ thể: mỗi lượt quét tạo các bản tóm tắt độc lập, Spark có thể đọc phân vùng theo ngày và tổng hợp nhiều sân bay song song\. Chạy Spark cục bộ chứng minh khả năng tái lập\. Nếu so sánh 1 và nhiều lõi trên cùng dữ liệu, ghi cả chi phí khởi động và quy mô mẫu; chỉ kết luận về cấu hình đã đo\.



### 4\. Pipeline Spark, quy tắc nghiệp vụ và kiểm thử — CLO3

|**Bước**|**Phần việc và đầu ra**|
|---|---|
|1\. Đọc với schema|Định nghĩa kiểu và trường bắt buộc cho quan trắc METAR, từng phiên bản dự báo và chỉ số ảnh do Satpy trích\. Gắn mã lô, khóa nguồn và thời gian nhận\.|
|2\. Kiểm tra, tách lỗi|Spark tách dòng sai kiểu, sân bay ngoài danh sách, thời gian sai hoặc giá trị vật lý không hợp lệ vào bảng quarantine có mã lỗi và tham chiếu đầu vào\. Tệp ảnh hỏng được bộ đọc ảnh ghi vào nhật ký lỗi tệp riêng\. Lập profile theo nguồn về số dòng, tỷ lệ thiếu, trùng và phân bố\.|
|3\. Biến đổi nghiệp vụ|- Chuẩn hóa tầm nhìn về mét, phân biệt mưa/dông tại trạm, hiện tượng lân cận và phần dự báo trong METAR\.<br>- Tính tuổi dữ liệu và chọn đúng phiên bản dự báo sẵn có tại thời điểm rà soát\.<br>- Ghép ảnh gần nhất đã nhận với sân bay và lượt rà soát\.<br>- Tổng hợp trạng thái, lý do và độ đầy đủ bằng chứng cho từng sân bay\.|
|4\. Quy tắc rà soát|Quy tắc thử: đưa lên đầu các trạm có METAR còn mới ghi nhận mưa/dông tại chỗ hoặc tầm nhìn dưới 5 km\. Nhóm tiếp theo gồm các trạm cần kiểm tra dữ liệu do bản tin quá 60 phút hoặc bị thiếu\. Sau đó là trạm có dự báo mưa trong giờ tới, rồi đến các trạm còn lại\. Spark gán cùng giá trị mức ưu tiên cho các sân bay thỏa cùng nhóm điều kiện\. Dashboard gắn nhãn đồng mức và hiển thị lý do riêng cho từng sân bay\. Đáp án của ca gồm tất cả sân bay thuộc mức cao nhất, độc lập với vị trí dòng trên màn hình\. Ghi riêng các nguồn thiếu\. Ảnh B13 cung cấp ngữ cảnh mây\. Các ngưỡng do nhóm đề xuất và phải trình bày khi demo\.|
|5\. Ghi kết quả|Ghi các bảng dữ liệu hợp lệ, quarantine, kết quả theo sân bay–thời điểm và bảng đối soát\. Giữ riêng bản sửa và phiên bản dự báo\. Chạy lại cùng đầu vào cho cùng số khóa kết quả\. Dashboard hiển thị nguồn, giờ nhận và trạng thái bằng chứng\.|



**Kiểm thử tối thiểu cho buổi 8**

- **Đối soát số lượng:** theo nguồn và lô, số dòng đọc = số dòng hợp lệ trước loại trùng \+ số dòng quarantine\. Đối soát tiếp số dòng hợp lệ với số phiên bản giữ lại và số dòng trùng hoàn toàn bị loại\. Lỗi tệp nhị phân được đếm theo tệp\.

- **Đối soát đầu ra:** mỗi thời điểm rà soát sinh đúng ba khóa sân bay–thời điểm, kể cả khi một nguồn thiếu\. Chạy lại bộ mẫu hai lần và so khóa, giá trị, số bản ghi lỗi\.

- **Kiểm thử thời gian:** một dự báo nhận sau thời điểm phát lại không được xuất hiện trong kết quả ở thời điểm đó\. Trình bày một ca dữ liệu đến trễ và một ca bản tin được sửa\.

- **Kiểm thử nghiệp vụ:** cho xem bản tin gốc của ca có mưa, giờ quan trắc, ảnh đã định vị và thứ tự rà soát\. Có ví dụ thiếu bản tin, mã tầm nhìn 9999 và một dòng sai schema\. Dữ liệu lỗi tạo để kiểm thử được ghi nhãn riêng\.

    

**Hai ca kiểm thử đã chốt\.** Các tình huống dưới đây dùng để kiểm tra quy tắc\. Khi đo KPI, lấy giờ bản tin từ dữ liệu của ca và ghi rõ ca thực hoặc ca mô phỏng\.

|**Ca tại thời điểm rà soát**|**Kết quả Spark và dashboard**|**Đáp án chấm**|
|---|---|---|
|15h: Đà Nẵng vừa ghi nhận dông kèm mưa; bản tin Nội Bài đã cũ 90 phút; Tân Sơn Nhất vừa báo thời tiết tốt, có dự báo mưa giờ tới\.|Đà Nẵng thuộc mức đầu\. Nội Bài có nhãn cần xác minh vì bản tin cũ\. Dự báo mưa tại Tân Sơn Nhất hiển thị cùng thời gian hiệu lực\.|Chọn Đà Nẵng, nêu dông kèm mưa tại trạm và chỉ đúng giờ quan trắc của METAR Đà Nẵng\.|
|Đà Nẵng có METAR mới ghi dông, tầm nhìn 6 km; Nội Bài có METAR mới ghi mưa, tầm nhìn 2 km\. Tân Sơn Nhất có bản tin mới và điều kiện bình thường\.|Đà Nẵng và Nội Bài cùng mức đầu\. Mỗi dòng dẫn bản tin của chính sân bay đó\.|Chấp nhận Đà Nẵng hoặc Nội Bài khi người thử nêu đúng lý do ưu tiên và giờ METAR tương ứng\. Đảo thứ tự hai dòng rồi chạy lại vẫn phải chấp nhận cả hai lựa chọn\.|



### 5\. Lịch 3 tuần gắn với các mốc của học phần

|Tuần và buổi|Công việc|Minh chứng phải có|
|---|---|---|
|**Tuần 1 · Buổi 1**|Khảo sát năng lực, giao vai\. Ghi 2–3 ý tưởng nội bộ, chẳng hạn ưu tiên rà soát theo ca, đối chiếu mưa dự báo, hoặc phân tích diễn biến mây sau ca\. Chọn bài toán đầu tiên và nêu lý do\. Bật lưu METAR và dự báo của ba sân bay từ ngày đầu\.|Biên bản chọn đề tài, bảng phân công, mẫu dữ liệu thật, nhật ký truy cập và phiên bản dự báo đầu tiên\.|
|**Tuần 1 · Buổi 3 — tiến độ 1**|Trình bày vấn đề, người quyết định, KPI, ba nguồn và 5V, sơ đồ đầu cuối, rủi ro\. Dựng một ảnh B13 đúng vị trí Đà Nẵng, đọc một mẫu bằng Spark\. Chốt mốc bắt đầu/kết thúc đợt thu liên tục trên máy nhóm và chuẩn bị phiếu đo KPI với người ngoài nhóm\.|Sơ đồ kiến trúc, từ điển bản đầu, ảnh có tọa độ, mẫu ba nguồn, phiếu đo KPI và danh sách rủi ro có người xử lý\.|
|**Tuần 2 · Buổi 4–5**|Hoàn thiện PySpark, bảng lỗi, chống trùng và ghép theo thời điểm\. Mở rộng ba sân bay, nối bảng kết quả vào giao diện\. Chọn ca phát lại, chuẩn bị bộ mẫu cục bộ và đối soát độ phủ theo nhật ký máy thu\.|Một lần chạy từ dữ liệu gốc tới bảng ba sân bay; profile dữ liệu, bảng đối soát đầu tiên, ảnh và bản tin truy nguyên được\.|
|**Tuần 2 · Buổi 6 — tiến độ 2**|Demo pipeline trên bộ mẫu gồm đầu vào đúng và sai, trình bày ma trận lưu trữ, số đo dung lượng, các việc còn lại và người phụ trách\. Chốt lĩnh vực và phạm vi\.|Notebook/mã nguồn chạy được, bảng hợp lệ/lỗi/kết quả, ma trận, backlog và biên bản chốt\. Theo hướng dẫn, thay đổi lớn sau mốc này cần giảng viên chấp thuận\.|
|**Tuần 3 · Buổi 7–8 — nghiệm thu kỹ thuật**|Thành viên khác cài và chạy lại từ hướng dẫn\. Thử mất nguồn và chạy lại, đối soát số liệu\. Mời người ngoài nhóm chưa dùng dashboard, hướng dẫn đọc METAR và đo KPI trên các ca đã review, gồm cả ca đồng mức\. Sau đó diễn tập demo trực tiếp và mở bản dự phòng ngoại tuyến\.|Kết quả test và đối soát, số liệu KPI kèm mẫu đo, giới hạn thử nghiệm, demo đầu cuối và video/ảnh dự phòng\.|
|**Cuối tuần 3 — bảo vệ**|Chốt báo cáo, slide, bộ dữ liệu và mã nguồn\. Review chéo, ghi nhận đóng góp, đánh giá đồng đẳng, tập vấn đáp ngoài phần từng người phụ trách\.|Đủ năm nhóm sản phẩm bàn giao, nhật ký ba tuần, khai báo AI, trích dẫn và hướng dẫn chạy lại\.|



### 6\. Phân công, rủi ro và roadmap — CLO4

|**Người**|**Vai trò gộp trong nhóm 3 người**|**Người review**|
|---|---|---|
|A|Điều phối \& nghiệp vụ, phụ trách KPI, kịch bản đo, giá trị, chi phí, tổng hợp báo cáo và slide|C rà lại KPI/luồng demo, B kiểm tra căn cứ chi phí<br>|
|B|Kiến trúc \& quản trị dữ liệu, phụ trách nguồn, xử lý ảnh, từ điển, quyền sử dụng và ma trận lưu trữ|C kiểm tra dữ liệu/kiến trúc, A kiểm tra lập luận lựa chọn|
|C|Xử lý dữ liệu, phụ trách PySpark, quarantine, test và hướng dẫn chạy\. Cùng A phụ trách phân tích \& trình diễn|B chạy lại pipeline, A đối chiếu đầu ra với quyết định<br>|



Mỗi sản phẩm quan trọng ghi người làm và ít nhất một người review khác\. Nhật ký tuần có nhiệm vụ, người phụ trách, minh chứng \(commit, kết quả chạy, bảng đo hoặc biên bản\), trạng thái và trở ngại\. Cuối kỳ, mỗi người tự đánh giá, ghi bằng chứng đóng góp và nhận đánh giá từ đồng đội\. Mỗi thành viên diễn tập giải thích cả lựa chọn lưu trữ, một bước Spark và một kết quả nghiệp vụ\.



|**Rủi ro trong 3 tuần**|**Người xử lý và hành động**|
|---|---|
|Máy cá nhân chậm khi tải nhiều kênh hoặc phân đoạn<br>|B chỉ chọn B13 và phân đoạn phủ vùng; đo 24 giờ trước khi tải bù, giữ bộ mẫu nhỏ để bảo vệ\.|
|Thiếu dữ liệu hoặc ngày bảo vệ mất mạng|B ghi độ phủ thực; C giữ bộ mẫu cục bộ kiểm tra được; A chuẩn bị video/ảnh dự phòng\.|
|Đánh giá thiên lệch vì người thử nhớ ca, ít ca mưa hoặc nguồn trễ|A luân phiên cách thử trên ca tương đương, ghi cỡ mẫu và số ca thiếu; C kiểm tra thời điểm dữ liệu khả dụng\.|
|Muốn mở rộng tính năng sát hạn|A đưa hạng mục mới vào backlog, ưu tiên minh chứng chấm điểm\. Thực hiện thủ tục với giảng viên nếu thay đổi lớn sau buổi 6\.|



**Roadmap sau bản thử:** khi có nhiều sân bay, thời gian lưu dài hoặc nhiều người truy cập, thử chuyển kho tệp sang object storage dùng chung, chạy Spark trên cụm và đo lại cùng KPI/chi phí\. Bước tiếp theo về nghiệp vụ là đánh giá cùng điều phối viên thực tế trước khi đề xuất triển khai\.



### 7\. Hồ sơ nộp và tự chấm



|**Sản phẩm theo hướng dẫn**|**Nội dung và giới hạn**|
|---|---|
|1\. Báo cáo toàn văn|**Tối đa 30 trang**, gồm vấn đề/quyết định/KPI, dữ liệu, kiến trúc và ma trận lưu trữ, Spark, kết quả/đối soát, giá trị/chi phí/rủi ro/roadmap, tài liệu tham khảo, nhật ký và đánh giá đóng góp\. Dự trù 26–28 trang để còn chỗ cho minh chứng\.|
|2\. Bộ dữ liệu|Dữ liệu mẫu hoặc liên kết tải; nguồn/điều kiện sử dụng; từ điển đủ sáu thuộc tính; profile và quy tắc chất lượng\.|
|3\. Bản thử nghiệm|Notebook hoặc mã nguồn Spark, cấu hình, đầu vào, đầu ra, quarantine, test/đối soát, phiên bản môi trường và hướng dẫn cài/chạy lại\.|
|4\. Slide bảo vệ|**Tối đa 10 slide**\. Phân bổ đề xuất: 1 vấn đề, 1 quyết định/KPI, 1 dữ liệu/5V, 1 kiến trúc, 1 ma trận, 2 pipeline/chất lượng, 1 kết quả, 1 demo, 1 chi phí/rủi ro/roadmap\.|
|5\. Minh chứng demo|Demo trực tiếp và **video/ảnh dự phòng không quá 3 phút**\. Kịch bản dự phòng: 30 giây giới thiệu quyết định, 60 giây chạy Spark và chỉ bảng lỗi, 60 giây đọc kết quả và truy nguyên, 30 giây đối soát và chốt KPI\.|



**Đối chiếu thang điểm MIS5007**



|Tiêu chí|Tỷ trọng|Minh chứng để mở khi vấn đáp|
|---|---|---|
|Vấn đề, quyết định và KPI|15%|Phiếu nghiệp vụ, công thức KPI, phiếu đo hai cách xem, tỷ lệ đạt và trung vị thời gian của lượt đạt|
|Dữ liệu và chất lượng|15%|Từ điển, profile, quy tắc, mẫu lỗi thật hoặc ca kiểm thử ghi nhãn|
|Kiến trúc và lựa chọn lưu trữ|20%|Sơ đồ đầu cuối, ma trận và giải thích đánh đổi|
|Pipeline Spark và tái lập|25%|Chạy PySpark từ mẫu theo hướng dẫn trên môi trường của thành viên review|
|Kết quả, kiểm thử và đối soát|10%|Bảng quyết định, kết quả test, số đếm đối soát và một ca truy nguyên|
|Trình bày và demo|15%|Slide, demo trực tiếp, bản dự phòng và vấn đáp cá nhân|



**Liêm chính học thuật\.** Trích nguồn dữ liệu, thư viện, hình ảnh, tài liệu và mã tham khảo\. Nhật ký AI ghi công cụ, mục đích, loại đầu ra và cách nhóm kiểm chứng\. Với bản kế hoạch này, ghi AI hỗ trợ soạn tiến độ và đề xuất thiết kế; nhóm đối chiếu hướng dẫn học phần, tự kiểm tra nguồn và tự thực hiện/giải thích các kết quả\. Phân biệt số đã đo với mục tiêu và giả định\.

