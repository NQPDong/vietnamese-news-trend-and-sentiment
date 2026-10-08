# ==============================================================================
# PIPELINE HUẤN LUYỆN SPARK MLLIB & GÁN NHÃN CẢM XÚC TẦNG SILVER HDFS (POWERSHELL)
# ==============================================================================

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " [BƯỚC 1/2] HUẤN LUYỆN SPARK MLLIB PIPELINE TRÊN SPARK MASTER" -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Cyan
docker exec spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 2G `
  --executor-memory 2G `
  --executor-cores 2 `
  /opt/spark-apps/src/models/train_sentiment_mllib.py

Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host " [KIỂM TRA HDFS] XÁC MINH ARTIFACT MODEL TẠI LAKEHOUSE" -ForegroundColor Yellow
Write-Host "==================================================================" -ForegroundColor Cyan
docker exec namenode hdfs dfs -ls /lakehouse/bd05_tin_tuc/models/sentiment_model

Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host " [BƯỚC 2/2] BATCH INFERENCE TRÊN 51.621 BÀI BÁO TẦNG SILVER" -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Cyan
docker exec spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 2G `
  --executor-memory 2G `
  --executor-cores 2 `
  /opt/spark-apps/src/jobs/silver_sentiment_inference.py

Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host " [KIỂM TRA HDFS] XÁC MINH TẦNG SILVER GÁN NHÃN HOÀN TẤT" -ForegroundColor Yellow
Write-Host "==================================================================" -ForegroundColor Cyan
docker exec namenode hdfs dfs -ls /lakehouse/bd05_tin_tuc/silver/news_sentiment.parquet
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " HOÀN TẤT TOÀN BỘ PIPELINE SPARK MLLIB SENTIMENT ANALYSIS!" -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Cyan
