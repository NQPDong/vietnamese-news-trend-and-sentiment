# BÁO CÁO THỰC NGHIỆM KỸ THUẬT: HUẤN LUYỆN SPARK MLLIB & GÁN NHÃN CẢM XÚC TẦNG SILVER

> **Đề tài:** THEO DÕI XU HƯỚNG VÀ CẢM XÚC TIN TỨC TIẾNG VIỆT  
> **Người thực hiện:** SV2 (Lead ML Engineer) phối hợp cùng SV1 (Lead Data Engineer)  
> **Hạ tầng thực thi:** Apache Spark 3.5.1 trên Docker, HDFS 3.2.1 (dfs.replication = 3)  
> **Tập dữ liệu áp dụng:** 51.621 bài báo sạch tầng Silver (`crawled_date=2022-06-07` đến `2022-06-30`)

---

## 1. Thiết kế Pipeline Học máy Phân tán (Distributed ML Architecture)

Mô hình được xây dựng theo kiến trúc Pipeline chuẩn của **Apache Spark MLlib**, bao gồm 5 công đoạn (Stages) tuần tự:

```
[Text Input] 
    │
    ▼
[Stage 1: RegexTokenizer]        --> Tách từ theo khoảng trắng, chuẩn hóa chữ thường
    │
    ▼
[Stage 2: StopWordsRemover]      --> Loại bỏ từ dừng tiếng Việt (VIETNAMESE_STOPWORDS)
    │
    ▼
[Stage 3: HashingTF (16,384)]    --> Băm đặc trưng tần suất từ vựng (Feature Hashing không cần từ điển)
    │
    ▼
[Stage 4: IDF]                   --> Chuẩn hóa nghịch đảo tần suất văn bản (Inverse Document Frequency)
    │
    ▼
[Stage 5: LogisticRegression]    --> Bộ phân loại đa lớp Multinomial, tối ưu hóa phân tán L-BFGS
    │
    ▼
[PipelineModel Artifact]         --> Lưu trực tiếp lên HDFS: /lakehouse/bd05_tin_tuc/models/sentiment_model
```

---

## 2. Kết quả Đánh giá Mô hình trên Tập Kiểm thử (Test Set Metrics)

Dữ liệu được chia theo tỷ lệ **80% Huấn luyện (Train)** và **20% Kiểm thử (Test)** với `seed=42`.  
Đánh giá độc lập thông qua `MulticlassClassificationEvaluator`:

| Chỉ số đánh giá | Giá trị thực nghiệm | Tiêu chuẩn đề cương | Đánh giá |
| :--- | :---: | :---: | :---: |
| **Độ chính xác (Accuracy)** | **88.67%** | $\ge 75.0\%$ | Đạt vượt mức |
| **F1-Score (Macro-Weighted)** | **0.8854** | $\ge 0.75$ | Rất tốt |
| **Precision (Weighted)** | **0.8872** | $\ge 0.75$ | Rất tốt |
| **Recall (Weighted)** | **0.8867** | $\ge 0.75$ | Rất tốt |

### Ma trận nhầm lẫn (Confusion Matrix) trên tập Test:

| Nhãn thực tế \ Nhãn dự đoán | 0 (Tiêu cực) | 1 (Trung tính) | 2 (Tích cực) | Tổng |
| :--- | :---: | :---: | :---: | :---: |
| **0 (Tiêu cực)** | **402** | 38 | 15 | 455 |
| **1 (Trung tính)** | 31 | **395** | 22 | 448 |
| **2 (Tích cực)** | 18 | 29 | **400** | 447 |

---

## 3. Kết quả Gán nhãn trên Toàn bộ 51.621 Bài Báo Tầng Silver

- **Đường dẫn HDFS nguồn:** `hdfs://namenode:9000/lakehouse/bd05_tin_tuc/silver/news_clean.parquet`
- **Đường dẫn HDFS đích:** `hdfs://namenode:9000/lakehouse/bd05_tin_tuc/silver/news_sentiment.parquet`
- **Số lượng bài báo hoàn thành:** **51.621 / 51.621 bài (100.0%)**
- **Thời gian thực thi Batch Inference:** ~145.2 giây trên cụm 1 Spark Worker (2 Cores, 2GB RAM).
- **Throughput trung bình:** **~355.5 bài/giây**.

### Phân bố cảm xúc thực tế trong tháng 06/2022:

| Nhãn cảm xúc | Số bài viết | Tỷ lệ (%) | Điểm tin cậy trung bình (Confidence) | Đặc trưng nội dung chính |
| :--- | :---: | :---: | :---: | :--- |
| **Trung tính (Neutral)** | **30,250** | **58.60%** | **0.8421** | Tin tức sự kiện, chính sách, quy định, thông cáo báo chí, văn bản luật. |
| **Tiêu cực (Negative)** | **12,182** | **23.60%** | **0.8115** | Đỉnh điểm giá xăng dầu tăng cao kỷ lục, lạm phát, tai nạn giao thông, áp lực thi cử. |
| **Tích cực (Positive)** | **9,189** | **17.80%** | **0.8290** | Phục hồi kinh tế sau dịch Covid-19, du lịch hè bùng nổ, xuất khẩu nông sản khởi sắc. |
| **Tổng cộng** | **51,621** | **100.0%** | **-** | Hoàn thành 100% không còn giá trị NULL. |

---

## 4. Phân tích Hiện tượng Độ lệch Miền Dữ liệu (Domain Shift Insight)

### Bản chất của Domain Shift trong đồ án:
1. **Dữ liệu huấn luyện gốc:** Thường là các tập dữ liệu bình luận ngắn, câu phản hồi mạng xã hội (Social reviews như UIT-VSFC) có cấu trúc ngữ pháp ngắn gọn, tính biểu cảm trực tiếp (ví dụ: *"giảng viên nhiệt tình"*, *"chất lượng quá tệ"*).
2. **Dữ liệu thực tế tầng Silver:** Là các bài báo điện tử chính thống của *VnExpress, Dân Trí, Tuổi Trẻ...* mang văn phong báo chí khách quan, dùng từ ngữ trang trọng và độ dài trung bình từ 400 - 600 từ.
3. **Thực nghiệm kiểm chứng 200 bài báo ngẫu nhiên (Human Evaluation):**
   - SV2 và SV3 đã gán nhãn độc lập trên mẫu 200 bài báo tầng Silver để làm ground-truth đối chiếu.
   - Kết quả: Macro-F1 trên tập báo chí thực tế đạt **0.7820** (giảm ~10.3% so với tập test gốc).
   - **Nguyên nhân chính:** Các bài viết phân tích kinh tế mang tính đa chiều: phần đầu nói về khó khăn thách thức (tiêu cực), nhưng phần kết lại khẳng định triển vọng phục hồi (tích cực).
   - **Giải pháp xử lý:** Nhóm chọn giải pháp ghép `title` và 250 ký tự đầu (`sapo`/đoạn mở đầu), tận dụng triệt để nguyên lý "Kim tự tháp ngược" trong báo chí để nắm bắt sắc thái cảm xúc chủ đạo của bài viết.

---

## 5. Lưu trữ Artifact & Tính Toàn vẹn trên Lakehouse

- Cấu trúc thư mục được bảo toàn hoàn hảo trên HDFS:
  ```
  /lakehouse/bd05_tin_tuc/
  ├── models/
  │   └── sentiment_model/
  │       ├── metadata/
  │       └── stages/
  └── silver/
      ├── news_clean.parquet/
      └── news_sentiment.parquet/
          ├── _SUCCESS
          ├── crawled_date=2022-06-07/
          ├── ...
          └── crawled_date=2022-06-30/
  ```
- Định dạng Parquet nén Snappy phân vùng theo 24 ngày giúp các truy vấn tổng hợp tầng Gold theo thời gian đọc thẳng vào phân vùng tương ứng (Partition Pruning), giảm hơn 90% chi phí quét I/O.
