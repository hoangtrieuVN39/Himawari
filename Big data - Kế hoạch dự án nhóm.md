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
|Himawari\-9, JMA/NOAA trên AWS|Kênh hồng ngoại B13 \($10.4\,\mu\text{m}$\), chu kỳ quét 10 phút\. Chỉ tải 2 phân đoạn cần cho ba sân bay: `S0410` \(phủ `VVDN`, `VVNB`\) và `S0510` \(phủ `VVTS`\), dung lượng $\sim 2.5\text{ MB}$/lượt quét\. Dùng `Satpy` định vị và cắt vùng 2 vòng \(Dual\-Box\): khung ngữ cảnh ngoài $\pm 1.0^\circ$ \(xuất ảnh PNG và tính chỉ số toàn vùng\) và khung tại trạm $\pm 0.25^\circ$ \(trích chỉ số đỉnh mây ngay trên sân bay\)\. Áp dụng chính sách dọn tệp gốc `.DAT.bz2` cũ hơn 24 giờ sau khi đã trích xuất xong ảnh PNG và JSON chỉ số, nhưng luôn ghim giữ \(pin\) các tệp thuộc cửa sổ phát lại\.|Kho công khai `noaa-himawari9`\. Ghi JMA/NOAA, ngày truy cập, đường dẫn tệp gốc và bước xử lý ảnh\. Ảnh nhóm đã cắt hoặc thêm chú giải cần ghi rõ nhãn sản phẩm đã xử lý \(*Processed Product*\)\. [Điều kiện phân phối](https://registry.opendata.aws/noaa-himawari/)|
|METAR, Aviation Weather Center|Gọi chung ba mã sân bay mỗi 10 phút\. Giữ giờ quan trắc, giờ nhận, bản tin gốc, mã hiện tượng và tầm nhìn\. Thu bổ sung bản tin gần đây từ cùng API để chọn ca phát lại\.|Ghi nguồn phát hành, URL, ngày lấy và giới hạn gọi\. [Tài liệu API](https://aviationweather.gov/data/api/) · [Điều kiện sử dụng NWS](https://www.weather.gov/disclaimer)|
|Open\-Meteo|Lưu dự báo lượng mưa theo giờ và giờ hiệu lực cho từng tọa độ\. Gọi mỗi giờ một lần mỗi sân bay, giữ phiên bản cũ\.|Dùng gói miễn phí cho đồ án phi thương mại, ghi công nguồn theo điều khoản\. [Điều kiện sử dụng](https://open-meteo.com/en/terms) · [Hạn mức](https://open-meteo.com/en/pricing)|



**5V cho báo cáo tiến độ 1\.** *Volume:* 7 ngày × 144 lượt quét/ngày = 1\.008 lượt dự kiến \(mỗi lượt 2 phân đoạn `S0410` và `S0510` $\approx 2.5\text{ MB}$\), ghi riêng số tệp và byte thực tải cùng dung lượng giữ lại sau chính sách dọn tệp gốc `.DAT.bz2` quá 24 giờ\. *Velocity:* ảnh, quan trắc và dự báo đến khác thời điểm\. *Variety:* tệp ảnh phân đoạn nhị phân `.DAT.bz2` và ảnh cắt `.png`, bản tin khí tượng METAR và JSON dự báo/chỉ số mây\. *Veracity:* nguồn đến trễ, ảnh thiếu phân đoạn hoặc hỏng tệp nhị phân, bản tin sửa, đơn vị và múi giờ\. *Value:* rút ngắn thời gian chọn sân bay cần rà soát và cho phép kiểm tra lý do\.



**Tập dữ liệu khả thi\.** Nhóm bố trí một máy chạy liên tục ít nhất 7 ngày, cả ngày lẫn đêm\. Trong hai tuần đầu, chốt trước một cửa sổ đo đủ 168 giờ để đánh giá độ phủ, tối đa 3\.024 cặp sân bay–lượt quét cho ba sân bay\. Ghi thời điểm bắt đầu/kết thúc theo UTC\. Theo từng ngày, đối soát ba số: lượt quét dự kiến, lượt có đủ 2 phân đoạn \(`S0410`, `S0510`\) cần dùng trên nguồn và lượt xử lý thành công\. Gắn nguyên nhân cho khoảng thiếu, phân biệt sự cố máy thu với thiếu tệp trên nguồn\. Đánh dấu các lượt tải bù để đo độ trễ trực tiếp riêng\. Đóng gói cố định \(pin\) cửa sổ phát lại 2–4 giờ \(ví dụ chuỗi 16 lượt quét 10 phút/lần từ `06:00 UTC` đến `08:30 UTC` ngày `2026-11-09`\) để chạy lại nhanh và giữ trọn chuỗi 6 ảnh trước sự kiện cho ca có mưa hoặc dông được METAR xác nhận, không bị xóa bởi tác vụ dọn tệp `.DAT.bz2` sau 24 giờ\. Nếu cần dùng ca lịch sử chưa có bản dự báo đã lưu, hiển thị rõ phần dự báo khuyết\. Chấm KPI thời gian trên các ca có đầu vào tương đương\.



**Từ điển dữ liệu\.** Mỗi trường có tên, kiểu, ý nghĩa/đơn vị, nguồn, quy tắc chất lượng và mức nhạy cảm\. Ví dụ: `observed_at_utc` là timestamp quan trắc METAR; `visibility_m` là số mét kèm cờ giới hạn khi METAR ghi 9999; `rain_mm` gắn với đúng khoảng tích lũy của dự báo; `b13_valid_fraction` là tỷ lệ điểm ảnh hợp lệ vật lý \($T_b \in [150\text{ K}, 340\text{ K}]$\) trong khung ngữ cảnh ngoài $\pm 1.0^\circ$, thuộc \[0,1\]; `inner_bt_min_c` là nhiệt độ sáng cực tiểu \($^\circ\text{C}$\) trong khung tại trạm $\pm 0.25^\circ$; `inner_cold_cloud_fraction` là tỷ lệ điểm ảnh mây đối lưu lạnh \($T_b < 235\text{ K}$, tức $-38.15^\circ\text{C}$\) trong khung tại trạm $\pm 0.25^\circ$; `inner_severe_convection_fraction` là tỷ lệ điểm ảnh đỉnh mây dông mạnh \($T_b < 220\text{ K}$, tức $-53.15^\circ\text{C}$\) trong khung tại trạm $\pm 0.25^\circ$\. Lưu thời điểm sự kiện và thời điểm hệ thống nhận dữ liệu riêng\. Bộ mẫu chỉ chứa dữ liệu khí tượng công khai; loại thông tin tài khoản và khóa truy cập khỏi hồ sơ nộp\.



### 3\. Kiến trúc và ma trận lưu trữ — CLO2



**Luồng xử lý:** S3/API → Python thu nhận phân đoạn `S0410`/`S0510`, METAR, Open\-Meteo và lưu bản gốc theo cơ chế **Dual\-Mode** \(ghi đĩa cục bộ kết hợp đẩy lên Hadoop HDFS qua WebHDFS `namenode:9870` / `hdfs://namenode:9000`\) → `Satpy` đọc kênh B13, định vị và cắt vùng 2 vòng \(Dual\-Box $\pm 1.0^\circ$ & $\pm 0.25^\circ$\), xuất ảnh PNG có chú giải và trích chỉ số JSON theo sân bay → PySpark đọc dữ liệu theo schema \(từ kho cục bộ hoặc `hdfs://namenode:9000`\) → kiểm tra và tách lỗi → chuẩn hóa, ghép và tổng hợp → bảng quyết định Parquet → Streamlit hiển thị và phát lại\.



**Phương án chính đề xuất \(Dual\-Mode: Kho tệp cục bộ \+ Hadoop HDFS \(Single\-Node Docker `namenode:9870` / `hdfs://namenode:9000`\) \+ Parquet\):** kết hợp kho tệp phân vùng theo lớp dữ liệu và ngày UTC (`date=YYYY-MM-DD`) trên đĩa cục bộ với cụm đơn nốt **Hadoop HDFS** chạy trong Docker Compose (`apache/hadoop:3`, NameNode UI cổng `9870`, RPC `hdfs://namenode:9000`, DataNode WebHDFS cổng `9864`)\. Hệ thống hỗ trợ **Auto Dual\-Write** \(ghi nguyên tử xuống `data/` cục bộ đồng thời đẩy lên HDFS qua WebHDFS REST API nếu cụm HDFS đang bật; tự động fallback không gián đoạn khi tắt HDFS để tiết kiệm RAM\) và công cụ đồng bộ bù **`sync_to_hdfs`** \(quét và đẩy toàn bộ `data/raw/`, `data/processed/`, `data/logs/` lên HDFS kèm ghi nhận thực nghiệm số tệp, tổng byte, độ trễ `elapsed_ms` và thông lượng `throughput_mb_s` vào `data/logs/hdfs_sync_log.jsonl`\)\. Bản gốc giữ ở định dạng nguồn, bảng chuẩn hóa và bảng kết quả lưu ở Parquet; PySpark là công cụ xử lý và Streamlit đọc bảng kết quả\.

|**Tiêu chí**|**Dual\-Mode: Kho tệp cục bộ \+ Hadoop HDFS \(Single\-Node Docker `namenode:9870` / `hdfs://namenode:9000`\) \+ Parquet \(đề xuất\)**|**PostgreSQL \(phương án so sánh\)**|
|---|---|---|
|Loại dữ liệu và workload|Lưu trực tiếp cả tệp nhị phân `.DAT.bz2`, ảnh `.png`, JSON gốc và bảng Parquet phân vùng `date=YYYY-MM-DD`\. Kho cục bộ giữ bản gốc cạnh các bảng phân vùng; Hadoop HDFS chuẩn hóa kiến trúc Data Lake phân tán, giao tiếp tự nhiên với PySpark \(`hdfs://namenode:9000`\) khi quét nhiều giờ/ngày để tái xử lý\.|Mạnh ở bảng có quan hệ \(OLTP\), truy vấn có chỉ mục và cập nhật từng dòng\. Không phù hợp lưu trực tiếp hàng nghìn tệp vệ tinh nhị phân `.DAT.bz2` và ảnh lớn vì làm tăng gánh nặng sao lưu\.|
|Độ trễ và hiệu năng|Kho cục bộ có độ trễ đọc/ghi thấp nhất trên 1 máy và làm bản dự phòng tức thời khi demo\. Nhánh HDFS qua WebHDFS/DataNode có thêm độ trễ mạng container, được đo thực nghiệm qua `Auto Dual-Write` và `sync_to_hdfs` trong `hdfs_sync_log.jsonl` \(`elapsed_ms`, `throughput_mb_s`\), nhưng sẵn sàng cho đọc song song nhiều DataNode\.|Thuận lợi cho tra cứu điểm theo sân bay–giờ có chỉ mục và nhiều người truy cập đồng thời, nhưng chậm hơn khi quét xử lý hàng loạt theo lô lớn\.|
|Mở rộng|Xử lý song song theo ngày hoặc lượt quét\. Mở rộng ngang liền mạch bằng cách thêm container/máy `datanode` và tăng `dfs.replication` trên HDFS hoặc chuyển sang object storage mà không phải sửa mã PySpark\.|Có chỉ mục, phân vùng dữ liệu, nhưng cần thiết kế sharding/partitioning và vận hành CSDL phức tạp khi dữ liệu phình to\.|
|Quản trị|Dùng danh mục tệp phân vùng, phiên bản, quyền thư mục, kiểm tra ghi nguyên tử \(`.tmp`\) và đồng bộ hai chiều qua `sync_to_hdfs`\. Nhóm tự xử lý chống trùng và tranh chấp ghi trong pipeline\.|Có giao dịch ACID, phân quyền và ràng buộc khóa\. Cần vận hành dịch vụ và sao lưu định kỳ\.|
|Chi phí và căn cứ chốt|Tận dụng máy nhóm\. Kho cục bộ chiếm $\sim 0\text{ MB}$ RAM nền; cụm HDFS Docker tiêu thụ thêm $\sim 1.5\text{ GB}$ RAM cho JVM của NameNode \+ DataNode nên cơ chế Dual\-Mode giúp bật/tắt linh hoạt\. Đo thực tế byte/ngày, RAM, tốc độ đồng bộ HDFS \(`hdfs_sync_log.jsonl`\) và thời gian đọc một ngày dữ liệu\.|Tiêu thụ $\sim 200\text{–}500\text{ MB}$ RAM nền cho dịch vụ CSDL, cộng thêm công cấu hình schema, đĩa và sao lưu\. Ghi rõ tiêu chí nào được đo, tiêu chí nào được đánh giá định tính\.|

Tại buổi 6, dùng số đo 24 giờ từ `ingestion_log.jsonl`, nhật ký đo thông lượng thực nghiệm `hdfs_sync_log.jsonl` và kết quả demo để chốt ma trận\. Báo cáo giải thích lợi ích phân tán theo công việc cụ thể: mỗi lượt quét tạo các bản tóm tắt độc lập, Spark có thể đọc phân vùng theo ngày trên kho cục bộ hoặc `hdfs://namenode:9000` và tổng hợp nhiều sân bay song song\. Chạy Spark và HDFS trong Docker chứng minh khả năng tái lập\. Nếu so sánh 1 và nhiều lõi trên cùng dữ liệu, ghi cả chi phí khởi động và quy mô mẫu; chỉ kết luận về cấu hình đã đo\.



### 4\. Pipeline Spark, quy tắc nghiệp vụ và kiểm thử — CLO3

|**Bước**|**Phần việc và đầu ra**|
|---|---|
|1\. Đọc với schema|Định nghĩa kiểu và trường bắt buộc cho quan trắc METAR, từng phiên bản dự báo và chỉ số ảnh Dual\-Box do `Satpy` trích \(`b13_valid_fraction`, `inner_bt_min_c`, `inner_cold_cloud_fraction`, `inner_severe_convection_fraction`\)\. Gắn mã lô, khóa nguồn và thời gian nhận\.|
|2\. Kiểm tra, tách lỗi|Spark tách dòng sai kiểu, sân bay ngoài danh sách, thời gian sai hoặc giá trị vật lý không hợp lệ \(như nhiệt độ sáng kênh B13 ngoài miền hợp lệ vật lý $[150\text{ K}, 340\text{ K}]$, tức $[-123.15^\circ\text{C}, +66.85^\circ\text{C}]$, hoặc tỷ lệ ngoài $[0, 1]$\) vào bảng quarantine có mã lỗi và tham chiếu đầu vào\. Tệp ảnh nhị phân `.DAT.bz2` bị hỏng hoặc cắt cụt được bộ đọc `Satpy` bắt lỗi và ghi vào nhật ký lỗi tệp riêng `data/logs/himawari_file_errors.jsonl` \(kèm cờ `is_synthetic_test` để phân biệt lỗi thực và lỗi tạo thử nghiệm\)\. Lập profile theo nguồn về số dòng, tỷ lệ thiếu, trùng và phân bố\.|
|3\. Biến đổi nghiệp vụ|- Chuẩn hóa tầm nhìn về mét, phân biệt mưa/dông tại trạm, hiện tượng lân cận và phần dự báo trong METAR\.<br>- Tính tuổi dữ liệu và chọn đúng phiên bản dự báo sẵn có tại thời điểm rà soát\.<br>- Ghép ảnh PNG và bộ chỉ số B13 Dual\-Box \($\pm 1.0^\circ$ và $\pm 0.25^\circ$\) gần nhất đã nhận với sân bay và lượt rà soát, phân loại mức mây đối lưu lạnh \($T_b < 235\text{ K}$ / $-38.15^\circ\text{C}$\) và đỉnh mây dông mạnh \($T_b < 220\text{ K}$ / $-53.15^\circ\text{C}$\)\.<br>- Tổng hợp trạng thái, lý do và độ đầy đủ bằng chứng cho từng sân bay\.|
|4\. Quy tắc rà soát|Quy tắc thử: đưa lên đầu các trạm có METAR còn mới ghi nhận mưa/dông tại chỗ hoặc tầm nhìn dưới 5 km\. Nhóm tiếp theo gồm các trạm cần kiểm tra dữ liệu do bản tin quá 60 phút hoặc bị thiếu\. Sau đó là trạm có dự báo mưa trong giờ tới, rồi đến các trạm còn lại\. Spark gán cùng giá trị mức ưu tiên cho các sân bay thỏa cùng nhóm điều kiện\. Dashboard gắn nhãn đồng mức và hiển thị lý do riêng cho từng sân bay\. Đáp án của ca gồm tất cả sân bay thuộc mức cao nhất, độc lập với vị trí dòng trên màn hình\. Ghi riêng các nguồn thiếu\. Ảnh B13 và các ngưỡng nhiệt độ sáng \($[150\text{ K}, 340\text{ K}]$, mây lạnh $<235\text{ K}$, đối lưu mạnh $<220\text{ K}$\) cung cấp ngữ cảnh mây tại trạm và vùng lân cận\. Các ngưỡng do nhóm đề xuất và phải trình bày khi demo\.|
|5\. Ghi kết quả|Ghi các bảng dữ liệu hợp lệ, quarantine, kết quả theo sân bay–thời điểm và bảng đối soát\. Giữ riêng bản sửa và phiên bản dự báo\. Chạy lại cùng đầu vào cho cùng số khóa kết quả\. Dashboard hiển thị nguồn, giờ nhận và trạng thái bằng chứng\.|



**Kiểm thử tối thiểu cho buổi 8**

- **Đối soát số lượng:** theo nguồn và lô, số dòng đọc = số dòng hợp lệ trước loại trùng \+ số dòng quarantine\. Đối soát tiếp số dòng hợp lệ với số phiên bản giữ lại và số dòng trùng hoàn toàn bị loại\. Lỗi tệp nhị phân `.DAT.bz2` được đếm riêng theo tệp trong `data/logs/himawari_file_errors.jsonl`\.

- **Đối soát đầu ra:** mỗi thời điểm rà soát sinh đúng ba khóa sân bay–thời điểm, kể cả khi một nguồn thiếu\. Chạy lại bộ mẫu hai lần và so khóa, giá trị, số bản ghi lỗi\.

- **Kiểm thử thời gian:** một dự báo nhận sau thời điểm phát lại không được xuất hiện trong kết quả ở thời điểm đó\. Trình bày một ca dữ liệu đến trễ và một ca bản tin được sửa\.

- **Kiểm thử nghiệp vụ:** cho xem bản tin gốc của ca có mưa, giờ quan trắc, ảnh B13 đã định vị Dual\-Box \($\pm 1.0^\circ$ & $\pm 0.25^\circ$\) và thứ tự rà soát\. Có ví dụ thiếu bản tin, mã tầm nhìn 9999, một dòng sai schema và một tệp ảnh `.DAT.bz2` hỏng sinh bởi cờ `--inject-corrupt-test`\. Dữ liệu lỗi tạo để kiểm thử được ghi nhãn riêng \(`"is_synthetic_test": true` trong `himawari_file_errors.jsonl`\)\.

    

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
|Máy cá nhân chậm hoặc đầy đĩa khi tải nhiều kênh/phân đoạn và chạy cụm lưu trữ<br>|B chỉ chọn kênh B13 và 2 phân đoạn `S0410` \(`VVDN`, `VVNB`\), `S0510` \(`VVTS`\) $\approx 2.5\text{ MB}$/lượt; tự động dọn tệp gốc `.DAT.bz2` đã xử lý quá 24 giờ nhưng ghim giữ \(pin\) cửa sổ phát lại; áp dụng Dual\-Mode để cho phép tạm dừng container HDFS khi cần tiết kiệm RAM và đồng bộ bù bằng `sync_to_hdfs`\.|
|Thiếu dữ liệu, tệp nhị phân hỏng hoặc ngày bảo vệ mất mạng|B ghi độ phủ thực trong `ingestion_log.jsonl` và bắt lỗi tệp hỏng vào `himawari_file_errors.jsonl`; C giữ bộ mẫu cục bộ kiểm tra được kèm cửa sổ phát lại cố định; A chuẩn bị video/ảnh dự phòng\.|
|Sai khác môi trường thư viện khí tượng \(`Satpy`, `pyresample`\), Spark và HDFS giữa máy các thành viên|B và C đóng gói trọn bộ môi trường vào `Dockerfile` và `docker-compose.yml` \(gồm container `namenode`, `datanode` dùng `apache/hadoop:3` và `ingestor`/`runner`\), đảm bảo cài đặt và tái lập kết quả đồng nhất khi nghiệm thu chéo ở buổi 7–8\.|
|Đánh giá thiên lệch vì người thử nhớ ca, ít ca mưa hoặc nguồn trễ|A luân phiên cách thử trên ca tương đương, ghi cỡ mẫu và số ca thiếu; C kiểm tra thời điểm dữ liệu khả dụng\.|
|Muốn mở rộng tính năng sát hạn|A đưa hạng mục mới vào backlog, ưu tiên minh chứng chấm điểm\. Thực hiện thủ tục với giảng viên nếu thay đổi lớn sau buổi 6\.|



**Roadmap sau bản thử:** khi có nhiều sân bay, thời gian lưu dài hoặc nhiều người truy cập, mở rộng cụm Hadoop HDFS nhiều DataNode hoặc chuyển kho tệp sang object storage dùng chung, chạy Spark trên cụm và đo lại cùng KPI/chi phí\. Bước tiếp theo về nghiệp vụ là đánh giá cùng điều phối viên thực tế trước khi đề xuất triển khai\.



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

