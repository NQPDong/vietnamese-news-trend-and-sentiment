#!/usr/bin/env bash
# ==============================================================================
# PIPELINE HUẤN LUYỆN SPARK MLLIB & GÁN NHÃN CẢM XÚC TẦNG SILVER HDFS
# ==============================================================================

set -e

echo "=================================================================="
echo " [BƯỚC 1/2] HUẤN LUYỆN SPARK MLLIB PIPELINE TRÊN SPARK MASTER"
echo "=================================================================="
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 2G \
  --executor-memory 2G \
  --executor-cores 2 \
  /opt/spark-apps/src/models/train_sentiment_mllib.py

echo ""
echo "=================================================================="
echo " [KIỂM TRA HDFS] XÁC MINH ARTIFACT MODEL TẠI LAKEHOUSE"
echo "=================================================================="
docker exec -it namenode hdfs dfs -ls /lakehouse/bd05_tin_tuc/models/sentiment_model

echo ""
echo "=================================================================="
echo " [BƯỚC 2/2] BATCH INFERENCE TRÊN 51.621 BÀI BÁO TẦNG SILVER"
echo "=================================================================="
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 2G \
  --executor-memory 2G \
  --executor-cores 2 \
  /opt/spark-apps/src/jobs/silver_sentiment_inference.py

echo ""
echo "=================================================================="
echo " [KIỂM TRA HDFS] XÁC MINH TẦNG SILVER GÁN NHÃN HOÀN TẤT"
echo "=================================================================="
docker exec -it namenode hdfs dfs -ls /lakehouse/bd05_tin_tuc/silver/news_sentiment.parquet
echo "=================================================================="
echo " HOÀN TẤT TOÀN BỘ PIPELINE SPARK MLLIB SENTIMENT ANALYSIS!"
echo "=================================================================="
