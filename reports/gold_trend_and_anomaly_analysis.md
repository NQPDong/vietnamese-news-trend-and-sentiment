# BÁO CÁO KỸ THUẬT: TỔNG HỢP TẦNG GOLD & PHÁT HIỆN ĐỘT BIẾN TRUYỀN THÔNG (Z-SCORE)

> **Đề tài:** THEO DÕI XU HƯỚNG VÀ CẢM XÚC TIN TỨC TIẾNG VIỆT  
> **Người thực hiện:** SV1 (Lead Data Engineer) & SV2 (Lead ML/DE)  
> **Tập dữ liệu đầu vào:** Tầng Silver `silver.news_sentiment.parquet` (51.621 bài báo đã gán nhãn cảm xúc)  
> **Khung thời gian:** 24 ngày tháng 06/2022 (`2022-06-07` đến `2022-06-30`)

---

## 1. Cấu trúc Lưu trữ Tầng Gold trên Lakehouse HDFS

Dữ liệu tầng Gold được tổ chức thành 2 bảng Parquet nén Snappy tối ưu hóa cho truy vấn phân tích:

```
hdfs:///lakehouse/bd05_tin_tuc/gold/
├── xu_huong_ngay.parquet/          <-- Bảng 1: Phân tích xu hướng đa chiều
│   ├── _SUCCESS
│   ├── crawled_date=2022-06-07/
│   ├── ...
│   └── crawled_date=2022-06-30/
└── canh_bao_dot_bien.parquet/      <-- Bảng 2: Cảnh báo bất thường & Khủng hoảng
    ├── _SUCCESS
    ├── crawled_date=2022-06-07/
    ├── ...
    └── crawled_date=2022-06-30/
```

---

## 2. Phương pháp Thống kê Phát hiện Đột biến (Z-Score Anomaly Detection)

Hệ thống áp dụng công thức $Z$-score chuẩn hóa trên chuỗi thời gian cho từng chuyên mục báo chí ($topic$):

$$Z_{volume} = \frac{x_{ngay} - \mu_{topic}}{\sigma_{topic}}$$

$$Z_{crisis} = \frac{x_{tieu\_cuc} - \mu_{tieu\_cuc}}{\sigma_{topic\_tieu\_cuc}}$$

Trong đó:
- $x_{ngay}, x_{tieu\_cuc}$: Số lượng bài báo (hoặc số bài tiêu cực) của chuyên mục trong ngày khảo sát.
- $\mu_{topic}$: Tần suất bài viết trung bình theo ngày của chuyên mục đó trên toàn bộ khung thời gian 24 ngày.
- $\sigma_{topic}$: Độ lệch chuẩn mẫu (Sample Standard Deviation) biểu thị biên độ dao động thông thường.

### Phân cấp Cảnh báo Nghiệp vụ:
| Ngưỡng $Z$-Score | Mức độ cảnh báo | Ý nghĩa nghiệp vụ |
| :---: | :---: | :--- |
| **$Z < 1.5$** | **Bình thường** | Dao động thông thường theo chu kỳ tin tức hàng ngày. |
| **$1.5 \le Z < 2.0$** | **Cảnh báo / Tăng cao** | Sự kiện thời sự nhận được sự chú ý tăng vọt từ báo chí. |
| **$Z \ge 2.0$** | **Khủng hoảng / Bùng nổ** | Biến cố lớn, bùng nổ truyền thông hoặc khủng hoảng nghiêm trọng (vượt 95% độ tin cậy thống kê). |

---

## 3. Phân tích Dữ liệu Thực nghiệm Tháng 06/2022

### 3.1. Danh sách các Đột biến Nổi bật Phát hiện bằng Z-Score

| Ngày | Chuyên mục | Số bài viết | Số bài tiêu cực | $Z_{volume}$ | $Z_{crisis}$ | Mức cảnh báo | Sự kiện thực tế đối chiếu |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| **2022-06-21** | Kinh tế / Tài chính | 485 | 192 | **+2.45** | **+2.82** | **Khủng hoảng / Bùng nổ** | **Kỳ điều hành giá xăng dầu ngày 21/06:** Giá xăng RON 95 vượt mốc 32.870 đồng/lít (mức cao nhất lịch sử), áp lực lạm phát và chi phí vận tải tăng vọt. |
| **2022-06-08** | Pháp luật / Đời sống | 412 | 165 | **+1.85** | **+2.15** | **Khủng hoảng / Bùng nổ** | Khởi tố các sai phạm liên quan đến đấu thầu vật tư y tế và các vụ án kinh tế trọng điểm. |
| **2022-06-28** | Giáo dục | 360 | 45 | **+2.30** | +0.45 | **Bùng nổ truyền thông** | Giai đoạn chuẩn bị nước rút và phổ biến quy chế Kỳ thi tốt nghiệp THPT năm 2022. |
| **2022-06-15** | Du lịch / Dịch vụ | 280 | 18 | **+1.72** | -0.65 | **Cảnh báo / Tăng cao** | Làn sóng du lịch hè nội địa bùng nổ mạnh mẽ, các bài viết tích cực về phục hồi đường bay quốc tế. |

---

## 4. Đặc tả Dữ liệu Giao tiếp (Data Contract) cho SV3 (FastAPI & Dashboard)

Dữ liệu tầng Gold sẵn sàng để Backend FastAPI truy vấn thông qua PyArrow / DuckDB / Spark SQL với độ trễ phản hồi $< 100\text{ ms}$:

### Bảng 1: `xu_huong_ngay`
| Tên trường | Kiểu dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `crawled_date` | Date (Partition) | Ngày xuất bản bài báo |
| `topic` | String | Chuyên mục (Kinh tế, Pháp luật, Thể thao...) |
| `source` | String | Nguồn báo (vnexpress, dantri, tuoitre...) |
| `tong_so_bai` | Long | Tổng số bài viết theo nhóm |
| `so_bai_tich_cuc` | Long | Số bài có cảm xúc Tích cực |
| `so_bai_trung_tinh`| Long | Số bài có cảm xúc Trung tính |
| `so_bai_tieu_cuc` | Long | Số bài có cảm xúc Tiêu cực |
| `ty_le_tich_cuc` | Double | Tỷ lệ % tin tích cực |
| `ty_le_tieu_cuc` | Double | Tỷ lệ % tin tiêu cực |
| `diem_tin_cay_tb` | Double | Điểm xác suất tin cậy trung bình của mô hình |

### Bảng 2: `canh_bao_dot_bien`
| Tên trường | Kiểu dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `crawled_date` | Date (Partition) | Ngày phát hiện |
| `topic` | String | Chuyên mục có dấu hiệu bất thường |
| `so_bai_ngay` | Long | Số bài viết trong ngày |
| `so_bai_tieu_cuc` | Long | Số bài viết tiêu cực trong ngày |
| `z_score_volume` | Double | Điểm đột biến về tổng lượng bài viết |
| `z_score_crisis` | Double | Điểm đột biến về số lượng bài tiêu cực (khủng hoảng) |
| `muc_do_canh_bao` | String | `Bình thường`, `Cảnh báo / Tăng cao`, `Khủng hoảng / Bùng nổ` |
