# HỆ THỐNG THEO DÕI XU HƯỚNG VÀ CẢNH BÁO ĐỘT BIẾN TRUYỀN THÔNG (06/2021 – 06/2022)
> **Môn học:** Dữ liệu lớn (Big Data)  
> **Đồ án:** Xây dựng Lakehouse xử lý & phân tích dòng tin tức báo điện tử Việt Nam  
> **Kiến trúc lưu trữ:** HDFS Lakehouse (`hdfs:///lakehouse/bd05_tin_tuc/`)

---

## 1. TỔNG QUAN HỆ THỐNG & DỮ LIỆU
- **Nguồn dữ liệu:** Vietnamese Online News Dataset (Kaggle - haitranquangofficial) thu thập từ các trang báo lớn: VnExpress, Dân Trí, Tuổi Trẻ, Thanh Niên, VietnamNet...
- **Khung thời gian nghiên cứu:** Từ `01/06/2021` đến `30/06/2022` (12 tháng biến động xã hội mạnh: đợt dịch Covid-19 lần 4, giãn cách CT16, tiêm chủng toàn quốc và mở cửa bình thường mới).
- **Kết quả lọc sơ bộ:** **59.168 bài báo** thỏa mãn mốc thời gian, được lưu trữ dưới định dạng Parquet tối ưu nén và phân vùng.

---

## 2. QUY CÁCH CẤU TRÚC THƯ MỤC LAKEHOUSE HDFS

Toàn bộ hệ thống lưu trữ phân tán nằm trong thư mục gốc HDFS: `hdfs:///lakehouse/bd05_tin_tuc/`

```text
hdfs:///lakehouse/bd05_tin_tuc/
├── bronze/         <- Dữ liệu thô nguyên trạng, append-only, nén Parquet/Delta
├── silver/         <- Bảng đã làm sạch, loại HTML, khử trùng lặp (SHA-256 + SimHash)
├── gold/           <- Bảng tổng hợp xu hướng theo giờ, chỉ số đột biến Z-score
├── features/       <- Ma trận vector đặc trưng TF-IDF, Word2Vec
├── models/         <- Lưu trữ artifacts mô hình MLlib đã huấn luyện
├── checkpoints/    <- Checkpoint offset cho Spark Structured Streaming
└── _quarantine/    <- Bản ghi lỗi định dạng, thiếu nội dung bị loại khỏi pipeline
```

---

## 3. YÊU CẦU TIỀN ĐỀ (PREREQUISITES)

1. **Hệ điều hành:** Windows 10/11 (hỗ trợ WSL 2).
2. **Phần mềm bắt buộc:**
   - [Docker Desktop](https://www.docker.com/products/docker-desktop/) (đã kích hoạt WSL 2 backend).
   - Python 3.10+ (đã có trên máy).
3. **Thư viện Python hỗ trợ xử lý dữ liệu cục bộ:**
   ```powershell
   pip install pandas pyarrow fastparquet
   ```

---

## 4. HƯỚNG DẪN CÀI ĐẶT & TRIỂN KHAI TỪNG BƯỚC

### BƯỚC 1: Cấu hình cụm Hadoop HDFS bằng Docker Compose
Tạo file `docker-compose.yml` tại thư mục gốc dự án:

```yaml
version: '3.8'

networks:
  lakehouse_net:
    name: lakehouse_net
    driver: bridge

volumes:
  hadoop_namenode_data:
  hadoop_datanode_data:

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    restart: always
    ports:
      - "9870:9870"   # Giao diện Web HDFS UI
      - "9000:9000"   # Cổng RPC HDFS Client
    environment:
      - CLUSTER_NAME=bd05_cluster
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
      - HDFS_CONF_dfs_replication=1
      - HDFS_CONF_dfs_permissions=false
    volumes:
      - hadoop_namenode_data:/hadoop/dfs/name
    networks:
      - lakehouse_net

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    restart: always
    ports:
      - "9864:9864"   # Giao diện Web DataNode
    environment:
      - SERVICE_PRECONDITION=namenode:9870
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
      - HDFS_CONF_dfs_replication=1
      - HDFS_CONF_dfs_permissions=false
    volumes:
      - hadoop_datanode_data:/hadoop/dfs/data
    networks:
      - lakehouse_net
    depends_on:
      - namenode
```

Khởi động cụm HDFS bằng lệnh:
```powershell
docker compose up -d
```
*Kiểm tra Web UI HDFS tại:* [http://localhost:9870](http://localhost:9870)

---

### BƯỚC 2: Khởi tạo 7 thư mục Lakehouse trên HDFS
Thực thi lệnh trực tiếp trên container `namenode` để tạo cấu trúc và phân quyền:

```powershell
docker exec namenode hdfs dfs -mkdir -p /lakehouse/bd05_tin_tuc/bronze
docker exec namenode hdfs dfs -mkdir -p /lakehouse/bd05_tin_tuc/silver
docker exec namenode hdfs dfs -mkdir -p /lakehouse/bd05_tin_tuc/gold
docker exec namenode hdfs dfs -mkdir -p /lakehouse/bd05_tin_tuc/features
docker exec namenode hdfs dfs -mkdir -p /lakehouse/bd05_tin_tuc/models
docker exec namenode hdfs dfs -mkdir -p /lakehouse/bd05_tin_tuc/checkpoints
docker exec namenode hdfs dfs -mkdir -p /lakehouse/bd05_tin_tuc/_quarantine
docker exec namenode hdfs dfs -chmod -R 777 /lakehouse
```

---

### BƯỚC 3: Xử lý & Lọc dữ liệu Kaggle (06/2021 – 06/2022)
1. Tải file `news_dataset.json` từ Kaggle và đặt vào `data/raw_kaggle/`.
2. Chạy script lọc dữ liệu:
   ```powershell
   python scripts/filter_kaggle_data.py
   ```
   *Kết quả xuất ra file:* `data/filtered_2021_2022/news_2021_2022.parquet` gồm **59.168 bài báo**.

---

### BƯỚC 4: Nạp dữ liệu vào tầng Bronze trên HDFS
Đưa file dữ liệu Parquet đã lọc vào tầng `bronze/` của Lakehouse:

```powershell
# 1. Copy file từ máy host vào container
docker cp data/filtered_2021_2022/news_2021_2022.parquet namenode:/tmp/news_2021_2022.parquet

# 2. Đẩy file từ container lên HDFS
docker exec namenode hdfs dfs -put /tmp/news_2021_2022.parquet /lakehouse/bd05_tin_tuc/bronze/
```

---

## 5. KIỂM TRA & NGHIỆM THU HỆ THỐNG

### Lệnh kiểm tra danh sách file và thư mục trên HDFS:
```powershell
docker exec namenode hdfs dfs -ls -R /lakehouse/bd05_tin_tuc/
```

**Kết quả hiển thị mong đợi:**
```text
drwxrwxrwx   - root supergroup          0 2026-09-17 06:38 /lakehouse/bd05_tin_tuc/_quarantine
drwxrwxrwx   - root supergroup          0 2026-09-17 07:19 /lakehouse/bd05_tin_tuc/bronze
-rw-r--r--   1 root supergroup   98099200 2026-09-17 07:19 /lakehouse/bd05_tin_tuc/bronze/news_2021_2022.parquet
drwxrwxrwx   - root supergroup          0 2026-09-17 06:38 /lakehouse/bd05_tin_tuc/checkpoints
drwxrwxrwx   - root supergroup          0 2026-09-17 06:38 /lakehouse/bd05_tin_tuc/features
drwxrwxrwx   - root supergroup          0 2026-09-17 06:38 /lakehouse/bd05_tin_tuc/gold
drwxrwxrwx   - root supergroup          0 2026-09-17 06:38 /lakehouse/bd05_tin_tuc/models
drwxrwxrwx   - root supergroup          0 2026-09-17 06:38 /lakehouse/bd05_tin_tuc/silver
```

### Kiểm tra dung lượng trên HDFS:
```powershell
docker exec namenode hdfs dfs -du -h /lakehouse/bd05_tin_tuc/
```

### Kiểm tra qua giao diện Web:
Truy cập: **http://localhost:9870** ➔ **Utilities** ➔ **Browse the file system** ➔ Điều hướng đến `/lakehouse/bd05_tin_tuc/bronze/`.
```
---

File README này đã tổng hợp đầy đủ từ kiến trúc, cấu hình Docker, script xử lý đến các lệnh kiểm tra thực tế bạn vừa chạy thành công.