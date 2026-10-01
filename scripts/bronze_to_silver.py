import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    return SparkSession.builder \
        .appName("Lakehouse_ETL_Bronze_to_Silver") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def main():
    start_time = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = "hdfs://namenode:9000/lakehouse/bd05_tin_tuc/bronze/news_2021_2022.parquet"
    silver_path = "hdfs://namenode:9000/lakehouse/bd05_tin_tuc/silver/news_clean.parquet"

    print("=" * 60)
    print(" BẮT ĐẦU PIPELINE ETL: BRONZE -> SILVER")
    print(f" Nguồn : {bronze_path}")
    print(f" Đích  : {silver_path}")
    print("=" * 60)

    # 1. ĐỌC DỮ LIỆU TẦNG BRONZE
    print("\n[Bước 1/5] Đang đọc dữ liệu Bronze từ HDFS...")
    raw_df = spark.read.parquet(bronze_path)
    initial_count = raw_df.count()
    print(f"-> Tổng số bản ghi thô (Bronze): {initial_count:,}")

    # 2. DATA QUALITY CHECK & LÀM SẠCH VĂN BẢN
    print("\n[Bước 2/5] Đang thực hiện kiểm tra chất lượng và làm sạch văn bản...")
    
    html_regex = r"<[^>]+>"
    whitespace_regex = r"\s+"

    cleaned_df = raw_df \
        .filter(F.col("title").isNotNull() & (F.length(F.trim(F.col("title"))) > 5)) \
        .filter(F.col("content").isNotNull() & (F.length(F.trim(F.col("content"))) >= 50)) \
        .filter(F.col("crawled_at").isNotNull()) \
        .withColumn("title_clean", F.trim(F.regexp_replace(F.regexp_replace(F.col("title"), html_regex, " "), whitespace_regex, " "))) \
        .withColumn("content_clean", F.trim(F.regexp_replace(F.regexp_replace(F.col("content"), html_regex, " "), whitespace_regex, " "))) \
        .withColumn("source", F.coalesce(F.trim(F.lower(F.col("source"))), F.lit("unknown"))) \
        .withColumn("topic", F.coalesce(F.trim(F.col("topic")), F.lit("Khác"))) \
        .withColumn("crawled_date", F.to_date(F.col("crawled_at")))

    valid_count = cleaned_df.count()
    filtered_out_count = initial_count - valid_count
    print(f"-> Số bản ghi hợp lệ sau khi lọc rác/rỗng: {valid_count:,}")
    print(f"-> Số bản ghi bị loại (Quarantine/Dirty): {filtered_out_count:,}")

    # 3. KHỬ TRÙNG LẶP (DEDUPLICATION)
    print("\n[Bước 3/5] Đang tiến hành khử trùng lặp bản ghi...")
    # Khử trùng theo URL trước (nếu trùng URL thì chắc chắn cùng bài)
    dedup_df = cleaned_df.dropDuplicates(["url"])
    # Khử trùng tiếp theo cặp tiêu đề sạch và nguồn báo
    dedup_df = dedup_df.dropDuplicates(["title_clean", "source"])

    final_count = dedup_df.count()
    duplicate_count = valid_count - final_count
    print(f"-> Số bản ghi trùng lặp bị loại bỏ: {duplicate_count:,}")
    print(f"-> Số bản ghi đạt chuẩn vào Silver: {final_count:,} ({(final_count/initial_count)*100:.2f}%)")

    # 4. CHỌN LỌC SCHEMA CHUẨN CHO TẦNG SILVER
    final_silver_df = dedup_df.select(
        F.col("id"),
        F.col("url"),
        F.col("source"),
        F.col("topic"),
        F.col("author"),
        F.col("title_clean").alias("title"),
        F.col("content_clean").alias("content"),
        F.col("picture_count"),
        F.col("crawled_at"),
        F.col("crawled_date")
    )

    # 5. GHI DỮ LIỆU SANG TẦNG SILVER PHÂN VÙNG THEO NGÀY
    print(f"\n[Bước 4/5] Đang ghi phân vùng theo 'crawled_date' vào Silver ({silver_path})...")
    final_silver_df.write \
        .mode("overwrite") \
        .partitionBy("crawled_date") \
        .parquet(silver_path)

    print("\n[Bước 5/5] Hoàn thành ghi dữ liệu Silver lên HDFS!")

    # Thống kê phân bố theo ngày
    print("\n--- THỐNG KÊ PHÂN BỐ BẢN GHI THEO NGÀY (TOP 10 NGÀY) ---")
    date_dist = final_silver_df.groupBy("crawled_date").count().orderBy(F.col("crawled_date").desc())
    date_dist.show(10, truncate=False)

    total_time = time.time() - start_time
    print("=" * 60)
    print(f" PIPELINE HOÀN TẤT THÀNH CÔNG TRONG {total_time:.2f} GIÂY")
    print(" Tầng Silver hiện sẵn sàng phục vụ cho Spark MLlib và Dashboard!")
    print("=" * 60)

    spark.stop()

if __name__ == "__main__":
    main()
