from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("TestSparkHDFS") \
        .getOrCreate()

    hdfs_path = "hdfs://namenode:9000/lakehouse/bd05_tin_tuc/bronze/news_2021_2022.parquet"
    print(f"Reading from: {hdfs_path}")
    
    df = spark.read.parquet(hdfs_path)
    count = df.count()
    print("=" * 50)
    print(f"SUCCESS! Total records in HDFS Bronze: {count:,}")
    print("Schema:")
    df.printSchema()
    print("Sample 3 records:")
    df.show(3, truncate=50)
    print("=" * 50)
    
    spark.stop()

if __name__ == "__main__":
    main()
