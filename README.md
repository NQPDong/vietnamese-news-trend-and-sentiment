# HỆ THỐNG LAKEHOUSE THEO DÕI XU HƯỚNG VÀ CẢM XÚC TIN TỨC TIẾNG VIỆT

> **Môn học:** Dữ liệu lớn (Big Data)  
> **Kiến trúc:** Lakehouse Medallion (HDFS 3 DataNodes • Apache Spark 3.5 MLlib • Parquet • Streamlit Dashboard)  
> **Dữ liệu thực nghiệm:** Vietnamese Online News Dataset (Tháng 06/2022 - 59.168 bài báo thô ➔ 51.621 bài báo sạch)  

---

## 1. TỔNG QUAN KIẾN TRÚC HỆ THỐNG

Hệ thống được thiết kế theo mô hình **Lakehouse Medallion (Bronze ➔ Silver ➔ Gold)** kết hợp cụm tính toán phân tán **Apache Spark** và cụm lưu trữ phân tán **Hadoop HDFS**:

```text
                     ┌───────────────────────────┐
                     │ Nguồn dữ liệu báo điện tử │ (59.168 bài báo thô)
                     └─────────────┬─────────────┘
                                   │
                                   ▼ (Ingestion / Replication = 3)
┌─────────────────────────────────────────────────────────────────────────────┐
│                              HADOOP HDFS CLUSTER                            │
│                  (1 NameNode: 9000/9870  •  3 DataNodes: 9864-9866)         │
│                                                                             │
│  ┌────────────────────┐   PySpark Data Quality   ┌────────────────────────┐ │
│  │    TẦNG BRONZE     │ ───────────────────────> │      TẦNG SILVER       │ │
│  │ (Dữ liệu thô Parquet)│  Loại HTML & Dedup     │ (51.621 bài sạch, 0 Null│ │
│  └────────────────────┘                          │  24 phân vùng theo ngày)│ │
│                                                  └───────────┬────────────┘ │
│                                                              │              │
│  ┌────────────────────┐   Aggregation & Z-score  ┌───────────▼────────────┐ │
│  │     TẦNG GOLD      │ <─────────────────────── │  SPARK MLLIB PIPELINE  │ │
│  │ - xu_huong_ngay    │    Window Functions      │ - RegexTokenizer       │ │
│  │ - canh_bao_dot_bien│                          │ - StopWordsRemover     │ │
│  └─────────┬──────────┘                          │ - HashingTF + IDF      │ │
│            │                                     │ - LogisticRegression   │ │
│            │                                     └────────────────────────┘ │
└────────────┼────────────────────────────────────────────────────────────────┘
             │
             ▼
┌───────────────────────────┐
│     STREAMLIT GUI         │ ➔ Giao diện trực quan hóa thời gian thực
│  (http://localhost:8501)  │ ➔ Biểu đồ xu hướng, cơ cấu cảm xúc, cảnh báo Z-score
└───────────────────────────┘
```

---

## 2. CẤU TRÚC THƯ MỤC DỰ ÁN

Dự án được chuẩn hóa phân lớp rõ ràng, loại bỏ toàn bộ các file rác và trùng lặp:

```text
D:\BigData\
├── docker-compose.yml              # Khởi tạo cụm phân tán: NameNode, 3 DataNodes, Spark Master/Worker
├── README.md                       # Tài liệu hướng dẫn tổng quan và vận hành đồ án
├── docs/                           # Tài liệu đề cương, ghi chú yêu cầu của thầy và phân công
│   ├── phan_cong.docx              # Phân công trách nhiệm 4 thành viên
│   ├── huong_dan_cai_dat_hdfs.md   # Hướng dẫn chi tiết triển khai cụm HDFS
│   └── notes.txt                   # Ghi chú 
├── reports/                        # Báo cáo thực nghiệm kỹ thuật và đo lường
│   ├── eda_summary.md              # Khám phá dữ liệu 59.168 bài báo
│   ├── benchmark_udf.md            # Benchmark Pandas UDF vs Standard UDF
│   ├── mllib_sentiment_evaluation.md # Đánh giá mô hình học máy phân tích cảm xúc
│   └── gold_trend_and_anomaly_analysis.md # Phân tích xu hướng và cảnh báo đột biến Z-Score
├── src/                            # Mã nguồn ứng dụng (Python packages chuẩn)
│   ├── nlp/                        # Làm sạch HTML, tách từ pyvi ViTokenizer, stopwords
│   ├── dedup/                      # Thuật toán khử trùng lặp dữ liệu (Exact Deduplication)
│   ├── models/                     # Script huấn luyện mô hình Spark MLlib
│   ├── jobs/                       # Toàn bộ các Spark Jobs ETL chuyển dịch Lakehouse
│   │   ├── bronze_to_silver_job.py # Job 1: Làm sạch và khử trùng lặp dữ liệu
│   │   ├── silver_sentiment_inference.py # Job 2: Gán nhãn cảm xúc bằng MLlib
│   │   └── silver_to_gold_job.py   # Job 3: Tổng hợp bảng Gold và tính điểm Z-score
│   ├── dashboard/                  # Giao diện trực quan hóa dữ liệu (GUI Streamlit)
│   │   └── app.py                  # Mã nguồn ứng dụng Dashboard
│   └── utils/                      # Tiện ích tạo SparkSession tối ưu
├── scripts/                        # Các kịch bản chạy tự động 1-Click
│   ├── run_all_pipeline.ps1        # Kịch bản chạy toàn bộ pipeline End-to-End
│   ├── run_dashboard.ps1           # Kịch bản khởi động Dashboard
│   ├── simulate_news_stream.py     # Script giả lập sinh luồng tin tức mới
│   └── filter_kaggle_data.py       # Script trích xuất dữ liệu gốc ban đầu
└── data/                           # Lưu trữ dữ liệu tĩnh (được .gitignore bảo vệ)
```

---

## 3. KẾT QUẢ ĐO LƯỜNG VÀ NGHIỆM THU THỰC TẾ

| Hạng mục | Số liệu thực nghiệm | Ghi chú kỹ thuật |
| :--- | :--- | :--- |
| **Dung lượng lưu trữ HDFS** | 3 DataNodes, `replication = 3` | Trạng thái `HEALTHY`, dung lượng trống ~2.78 TB |
| **Dữ liệu thô (Bronze)** | **59.168** bài báo | File `news_2021_2022.parquet` (97.4 MB, nén Snappy) |
| **Dữ liệu sạch (Silver)** | **51.621** bài báo | Lọc bỏ 7.547 bài rác/trùng lặp (12.75%), **0 giá trị NULL** |
| **Phân vùng dữ liệu (Partitions)** | **24 ngày** (07/06 - 30/06/2022) | Tối ưu hóa truy vấn Partition Pruning |
| **Mô hình Spark MLlib** | Pipeline: Tokenizer ➔ TF-IDF ➔ LogisticRegression | Phân loại 3 nhãn: Tích cực, Tiêu cực, Trung tính |
| **Phát hiện đột biến ($Z$-score)** | Phân vị $|Z| > 1.5$ và $|Z| > 2.0$ | Cảnh báo các sự kiện bùng nổ truyền thông theo chủ đề |

---

## 4. HƯỚNG DẪN KHỞI CHẠY VÀ DEMO (QUICK START)

### Bước 1: Khởi động cụm HDFS và Spark phân tán
Mở Windows PowerShell tại thư mục dự án và chạy:
```powershell
docker compose up -d
```
*Kiểm tra Web UI:*
- **HDFS NameNode UI:** [http://localhost:9870](http://localhost:9870)
- **Spark Master UI:** [http://localhost:8080](http://localhost:8080)

### Bước 2: Chạy toàn bộ Data Pipeline (1-Click Runner)
Chạy script điều phối tổng thể tự động:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_all_pipeline.ps1
```
*Script sẽ tự động thực thi 3 Jobs Spark tuần tự: Làm sạch ➔ Gán nhãn MLlib ➔ Tính toán tầng Gold.*

### Bước 3: Mở Giao diện Dashboard (GUI)
Khởi chạy giao diện trực quan hóa người dùng:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_dashboard.ps1
```
*Truy cập Dashboard tại:* **[http://localhost:8501](http://localhost:8501)**

### Bước 4: Giả lập sinh luồng tin tức mới (Streaming Simulation)
Chạy script mô phỏng luồng tin tức thời gian thực theo yêu cầu của giảng viên:
```powershell
.venv\Scripts\python scripts/simulate_news_stream.py
```

---

## 5. PHÂN CÔNG VAI TRÒ 4 THÀNH VIÊN

- **SV1 (Lead Data Engineer):** Thiết kế kiến trúc HDFS Lakehouse, phân vùng Parquet, xây dựng job `bronze_to_silver_job.py`.
- **SV2 (Lead ML/DE):** Tiền xử lý NLP tiếng Việt, huấn luyện mô hình phân loại cảm xúc `train_sentiment_mllib.py`, xây dựng thuật toán phát hiện đột biến $Z$-score.
- **SV3 (Lead App/Dashboard):** Thiết kế và lập trình giao diện trực quan hóa Streamlit (`src/dashboard/app.py`), tối ưu hóa hiển thị dữ liệu tầng Gold.
- **SV4 (Lead DevOps/Streaming):** Cấu hình Docker Compose (HDFS 3 DataNodes + Spark Cluster), viết script điều phối tự động `run_all_pipeline.ps1` và script giả lập luồng `simulate_news_stream.py`.
