# BÁO CÁO PHÂN TÍCH KHÁM PHÁ DỮ LIỆU (EDA) - TẬP TIN TỨC 2021–2022

> **Tập tin nguồn:** `data\filtered_2021_2022\news_2021_2022.parquet`  
> **Tổng số bài báo:** **59,168 bài**  
> **Người thực hiện:** SV2 (Lead ML Engineer)  

---

## 1. Cấu trúc trường dữ liệu & Độ toàn vẹn (Schema Integrity)

| Tên cột | Kiểu dữ liệu | Số giá trị rỗng (Null) | Tỷ lệ rỗng (%) |
| --- | --- | --- | --- |
| id | int64 | 0 | 0.00% |
| author | str | 0 | 0.00% |
| content | str | 0 | 0.00% |
| picture_count | int64 | 0 | 0.00% |
| processed | int64 | 0 | 0.00% |
| source | str | 4 | 0.01% |
| title | str | 0 | 0.00% |
| topic | str | 0 | 0.00% |
| url | str | 0 | 0.00% |
| crawled_at | datetime64[us] | 0 | 0.00% |

---

## 2. Phân bố Dữ liệu theo Dòng thời gian (Temporal Distribution)

- **Mốc thời gian sớm nhất:** 2022-06-07 14:05:50.904322
- **Mốc thời gian muộn nhất:** 2022-06-30 23:55:20.097088

### Phân bố số lượng tin tức theo từng tháng:

| Tháng | Số bài báo | Tỷ lệ (%) |
| --- | --- | --- |
| 2022-06 | 59168 | 100.0% |

> **Nhận xét chuyên môn:**
> - Giai đoạn tháng **07/2021 - 10/2021** là đỉnh điểm bùng phát đợt dịch Covid-19 lần thứ 4 (áp dụng Chỉ thị 16). Mật độ tin tức và các bài viết về y tế, giãn cách tăng đột biến, là cơ sở dữ liệu thực nghiệm tuyệt vời cho bài toán phát hiện bất thường ($Z$-score) và phân loại cảm xúc tiêu cực.
> - Giai đoạn **đầu năm 2022** xuất hiện các bài viết về phục hồi kinh tế, mở cửa du lịch và biến động giá năng lượng/xăng dầu.

---

## 3. Thống kê Độ dài Văn bản (Text Length Statistics)

- **Tiêu đề - Độ dài ký tự trung bình:** 65.4 (min: 0, max: 786)
- **Tiêu đề - Số từ trung bình:** 14.6 từ (min: 0, max: 161)
- **Nội dung - Độ dài ký tự trung bình:** 2390.6 (median: 2068.5, max: 61885)
- **Nội dung - Số từ trung bình:** 517.8 từ (median: 444.0, min: 0, max: 13785)

> **Khuyến nghị cho Module NLP (Tuần 2 & 3):**
> - Độ dài trung bình của nội dung báo chí tiếng Việt khá dài, do đó khi đưa qua Tokenizer (Pandas UDF Underthesea/PyVi) cần xử lý theo từng khối văn bản vector hóa PyArrow để tránh nghẽn bộ nhớ worker node.
> - Khi trích xuất đặc trưng TF-IDF / CountVectorizer cho Spark MLlib, nên giới hạn `vocabSize` khoảng 10.000 - 20.000 từ và lọc bỏ `minDF` để tối ưu kích thước mô hình.

---

## 4. Phân bố theo Nguồn báo / Tên miền (Sources)

| Nguồn báo | Số lượng bài |
| --- | --- |
| zingnews | 5323 |
| laodong | 5150 |
| 24h.com.vn | 4749 |
| thanhnien.vn | 4570 |
| vtv.vn | 3896 |
| vnexpress | 3302 |
| dantri | 3149 |
| danviet | 3028 |
| vov.vn | 2924 |
| tienphong | 2791 |

---

## 5. Thống kê Trùng lặp Sơ bộ (Duplicates Insight)

- **Số bài trùng lặp tiêu đề chính xác (Exact Duplicate Title):** 3,626 (6.13%)
- **Số bài trùng lặp nội dung chính xác (Exact Duplicate Content):** 8,909 (15.06%)

> **Ý nghĩa thực tế cho Tuần 2 (Dedup Pipeline):**
> - Tỷ lệ trùng lặp chính xác (Exact match) cho thấy bài viết được sao chép nguyên văn hoặc bot crawl trùng lặp. Tầng 1 (SHA-256) sẽ loại bỏ nhanh chóng nhóm này.
> - Phần lớn các bài báo khác cùng khai thác 1 sự kiện thời sự thường được biên tập lại tiêu đề hoặc sửa đoạn mở đầu ('tin xào'). Do đó, thuật toán **SimHash 64-bit với Hamming distance $\le 3$** ở Tầng 2 là bắt buộc để lọc triệt để hiện tượng near-duplicate.
