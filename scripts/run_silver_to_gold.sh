#!/usr/bin/env bash
# ==============================================================================
# PIPELINE TỔNG HỢP TẦNG GOLD & PHÁT HIỆN ĐỘT BIẾN Z-SCORE TRÊN HDFS
# ==============================================================================

set -e

echo "=================================================================="
echo " [CHẠY JOB] TỔNG HỢP BẢNG GOLD TRÊN SPARK MASTER"
echo "=================================================================="
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 2G \
  --executor-memory 2G \
  --executor-cores 2 \
  /opt/spark-apps/src/jobs/silver_to_gold_job.py

echo ""
echo "=================================================================="
echo " [KIỂM TRA HDFS] BẢNG 1: gold.xu_huong_ngay"
echo "=================================================================="
docker exec -it namenode hdfs dfs -ls /lakehouse/bd05_tin_tuc/gold/xu_huong_ngay.parquet

echo ""
echo "=================================================================="
echo " [KIỂM TRA HDFS] BẢNG 2: gold.canh_bao_dot_bien"
echo "=================================================================="
docker exec -it namenode hdfs dfs -ls /lakehouse/bd05_tin_tuc/gold/canh_bao_dot_bien.parquet
echo "=================================================================="
echo " HOÀN TẤT TỔNG HỢP TẦNG GOLD TRÊN LAKEHOUSE!"
echo "=================================================================="
