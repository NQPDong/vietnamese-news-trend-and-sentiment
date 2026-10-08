import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Đảm bảo import được các module trong src/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.nlp.stopwords import VIETNAMESE_STOPWORDS


def create_spark_session():
    """
    Tạo SparkSession tối ưu cho huấn luyện MLlib.
    """
    return SparkSession.builder \
        .appName("Train_Spark_MLlib_Sentiment") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def get_training_dataset(spark):
    """
    Tạo tập dữ liệu huấn luyện phân tích cảm xúc tiếng Việt 3 lớp:
      - 0: Tiêu cực (Negative)
      - 1: Trung tính (Neutral)
      - 2: Tích cực (Positive)
    Mẫu câu chuẩn bối cảnh kinh tế - xã hội - đời sống Việt Nam 2021-2022.
    """
    sample_corpus = [
        # --- 0: Tiêu cực (Negative) ---
        ("Doanh nghiệp phá sản hàng loạt do chi phí sản xuất tăng phi mã", 0),
        ("Thị trường chứng khoán giảm điểm sâu kỷ lục, nhà đầu tư thua lỗ nặng nề", 0),
        ("Giá xăng dầu tiếp tục tăng cao gây áp lực lạm phát trầm trọng", 0),
        ("Dịch bệnh bùng phát phức tạp khiến hoạt động kinh doanh bị đình trệ kéo dài", 0),
        ("Sai phạm nghiêm trọng trong đấu thầu thiết bị y tế bị khởi tố bắt tạm giam", 0),
        ("Hàng ngàn lao động mất việc làm do công ty cắt giảm đơn hàng hàng loạt", 0),
        ("Ách tắc giao thông nghiêm trọng và ô nhiễm không khí ở mức báo động", 0),
        ("Thiệt hại nặng nề sau cơn bão lũ lớn, nhiều ngôi nhà bị sập và ngập sâu", 0),
        ("Lừa đảo trực tuyến gia tăng mạnh chiếm đoạt tiền gửi ngân hàng của người dân", 0),
        ("Áp lực nợ xấu ngân hàng gia tăng đáng báo động trong quý này", 0),
        ("Xuất khẩu sụt giảm nghiêm trọng do suy thoái kinh tế toàn cầu và đứt gãy cung ứng", 0),
        ("Tai nạn giao thông thảm khốc trên tuyến quốc lộ khiến nhiều người thương vong", 0),
        ("Chi phí sinh hoạt đắt đỏ đè nặng lên vai người có thu nhập thấp", 0),
        ("Doanh số bán lẻ tụt dốc thảm hại, sức mua thị trường ảm đạm", 0),
        ("Tình trạng quá tải bệnh viện diễn biến xấu gây bức xúc dư luận", 0),

        # --- 1: Trung tính (Neutral) ---
        ("Hội nghị sơ kết công tác 6 tháng đầu năm được tổ chức tại Hà Nội", 1),
        ("Bộ Y tế ban hành hướng dẫn mới về tiêm chủng phòng dịch bệnh", 1),
        ("Ngân hàng Nhà nước công bố tỷ giá trung tâm ngày hôm nay", 1),
        ("Tổng cục Thống kê báo cáo chỉ số giá tiêu dùng CPI định kỳ tháng này", 1),
        ("Lịch thi tốt nghiệp trung học phổ thông quốc gia diễn ra vào cuối tháng 6", 1),
        ("Thời tiết các khu vực trên cả nước trong tuần này có mưa rào rải rác", 1),
        ("Ủy ban nhân dân thành phố họp thảo luận về quy hoạch sử dụng đất", 1),
        ("Bộ Giao thông Vận tải kiểm tra tiến độ dự án đường cao tốc Bắc Nam", 1),
        ("Đại học Quốc gia công bố điểm chuẩn kỳ thi đánh giá năng lực", 1),
        ("Chính phủ ban hành nghị định sửa đổi bổ sung về quản lý bất động sản", 1),
        ("Các đại biểu tham gia phiên chất vấn trực tiếp tại kỳ họp Quốc hội", 1),
        ("Bộ Công Thương theo dõi diễn biến cung cầu hàng hóa thiết yếu trên thị trường", 1),
        ("Giá vàng hôm nay đi ngang trong phiên giao dịch sáng đầu tuần", 1),
        ("Việt Nam và đối tác quốc tế trao đổi đoàn đại biểu cấp bộ ngành", 1),
        ("Thông báo tuyển dụng viên chức ngành giáo dục năm học mới", 1),

        # --- 2: Tích cực (Positive) ---
        ("Kinh tế Việt Nam phục hồi mạnh mẽ sau đại dịch vượt xa mọi dự báo", 2),
        ("Doanh thu du lịch tăng trưởng ấn tượng trong kỳ nghỉ lễ kích cầu thành công", 2),
        ("Kim ngạch xuất khẩu nông sản đạt kỷ lục mới nhờ mở rộng thị trường quốc tế", 2),
        ("Nhiều doanh nghiệp công nghệ Việt Nam ghi nhận lợi nhuận tăng trưởng vượt bậc", 2),
        ("Chính sách hỗ trợ kịp thời giúp người dân và doanh nghiệp nhanh chóng vượt khó", 2),
        ("Hạ tầng giao thông trọng điểm được khánh thành đưa vào sử dụng vượt tiến độ", 2),
        ("Thu hút dòng vốn đầu tư nước ngoài FDI khởi sắc mang lại hàng triệu việc làm mới", 2),
        ("Đội tuyển thể thao Việt Nam thi đấu xuất sắc giành trọn huy chương vàng", 2),
        ("Nhiều sáng kiến công nghệ xuất sắc mang lại hiệu quả kinh tế cao và giải thưởng lớn", 2),
        ("Đời sống người lao động được cải thiện đáng kể với mức thu nhập tăng cao", 2),
        ("Môi trường kinh doanh được cải thiện rõ rệt, thúc đẩy tinh thần khởi nghiệp", 2),
        ("Các bệnh nhân phục hồi sức khỏe hoàn toàn và xuất viện trong niềm vui lớn", 2),
        ("Sản lượng lương thực được mùa trúng giá đem lại niềm vui cho bà con nông dân", 2),
        ("Các đường bay quốc tế mở lại toàn diện tạo đà bứt phá cho ngành hàng không", 2),
        ("Chỉ số niềm tin kinh doanh tăng vọt báo hiệu làn sóng đầu tư mạnh mẽ", 2)
    ]

    # Nhân bản để tập huấn luyện đạt kích thước phân tán (~6.750 bản ghi)
    expanded_data = sample_corpus * 150

    schema = StructType([
        StructField("text", StringType(), False),
        StructField("label", IntegerType(), False)
    ])

    return spark.createDataFrame(expanded_data, schema=schema)


def build_pipeline():
    """
    Xây dựng Pipeline Spark MLlib phân tán:
    Tokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression
    """
    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="raw_words",
        pattern=r"\s+",
        toLowercase=True
    )

    stop_words_remover = StopWordsRemover(
        inputCol="raw_words",
        outputCol="filtered_words",
        stopWords=VIETNAMESE_STOPWORDS
    )

    hashing_tf = HashingTF(
        inputCol="filtered_words",
        outputCol="raw_features",
        numFeatures=16384
    )

    idf = IDF(
        inputCol="raw_features",
        outputCol="features"
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        family="multinomial",
        maxIter=50,
        regParam=0.01,
        elasticNetParam=0.0
    )

    return Pipeline(stages=[tokenizer, stop_words_remover, hashing_tf, idf, lr])


def main():
    start_time = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    model_hdfs_path = "hdfs://namenode:9000/lakehouse/bd05_tin_tuc/models/sentiment_model"

    print("=" * 65)
    print(" BẮT ĐẦU HUẤN LUYỆN SPARK MLLIB: SENTIMENT ANALYSIS (VIETNAMESE)")
    print(f" Đích lưu trữ Model HDFS: {model_hdfs_path}")
    print("=" * 65)

    print("\n[Bước 1/5] Khởi tạo dữ liệu huấn luyện và phân chia Train/Test (80/20)...")
    df = get_training_dataset(spark)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"-> Tập huấn luyện (Train): {train_df.count():,} bản ghi")
    print(f"-> Tập kiểm thử (Test)  : {test_df.count():,} bản ghi")

    print("\n[Bước 2/5] Đang khớp (fit) Spark MLlib Pipeline...")
    pipeline = build_pipeline()
    pipeline_model = pipeline.fit(train_df)
    print("-> Huấn luyện mô hình LogisticRegression hoàn tất!")

    print("\n[Bước 3/5] Đang dự đoán và đánh giá chất lượng mô hình trên Test...")
    predictions = pipeline_model.transform(test_df)

    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    evaluator_prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    evaluator_rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")

    acc = evaluator_acc.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)
    prec = evaluator_prec.evaluate(predictions)
    rec = evaluator_rec.evaluate(predictions)

    print("-" * 50)
    print(f" ĐỘ CHÍNH XÁC (Accuracy)  : {acc * 100:.2f}%")
    print(f" F1-SCORE (Macro-Weighted) : {f1:.4f}")
    print(f" PRECISION                 : {prec:.4f}")
    print(f" RECALL                    : {rec:.4f}")
    print("-" * 50)

    print("\n[Bước 4/5] Kết quả dự đoán mẫu trên tập kiểm thử:")
    predictions.select("text", "label", "prediction").show(6, truncate=60)

    print(f"\n[Bước 5/5] Đang lưu trữ PipelineModel lên HDFS: {model_hdfs_path}...")
    pipeline_model.write().overwrite().save(model_hdfs_path)
    print("-> Đã lưu mô hình thành công lên HDFS!")

    total_time = time.time() - start_time
    print("=" * 65)
    print(f" HOÀN TẤT HUẤN LUYỆN TRONG {total_time:.2f} GIÂY")
    print("=" * 65)

    spark.stop()


if __name__ == "__main__":
    main()
