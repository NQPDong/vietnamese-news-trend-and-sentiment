from pyspark.sql import SparkSession

def get_spark_session(app_name="Lakehouse_App", master="spark://spark-master:7077"):
    """
    Tạo hoặc lấy SparkSession chuẩn hóa cho toàn bộ hệ thống Lakehouse HDFS.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    if master:
        builder = builder.master(master)
        
    return builder.getOrCreate()
