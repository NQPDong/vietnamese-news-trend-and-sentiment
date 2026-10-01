import os
import pandas as pd

# Đường dẫn file
input_path = os.path.join("data", "raw_kaggle", "news_dataset.json")
output_dir = os.path.join("data", "filtered_2021_2022")
output_path = os.path.join(output_dir, "news_2021_2022.parquet")

print("1. Đang kiểm tra file đầu vào...")
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Không tìm thấy file: {input_path}")

os.makedirs(output_dir, exist_ok=True)

print("2. Đang đọc dữ liệu từ JSON (dung lượng ~655MB, có thể mất 10-20 giây)...")
df = pd.read_json(input_path)

print("3. Đang chuẩn hóa cột ngày 'crawled_at'...")
df["crawled_at"] = pd.to_datetime(df["crawled_at"], errors="coerce")

# Lọc trong khoảng thời gian đồ án yêu cầu: 01/06/2021 đến 30/06/2022
start_date = "2021-06-01 00:00:00"
end_date = "2022-06-30 23:59:59"

print(f"4. Đang lọc dữ liệu từ {start_date} đến {end_date}...")
filtered_df = df[(df["crawled_at"] >= start_date) & (df["crawled_at"] <= end_date)].copy()

print(f"-> Số lượng bài báo sau lọc: {len(filtered_df):,} bài")

print("5. Đang lưu sang định dạng Parquet...")
filtered_df.to_parquet(output_path, engine="pyarrow", index=False)

print(f"✓ Đã lưu thành công tại: {output_path}")
