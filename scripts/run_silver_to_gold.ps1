# ==============================================================================
# PIPELINE TỔNG HỢP TẦNG GOLD & PHÁT HIỆN ĐỘT BIẾN Z-SCORE TRÊN HDFS (POWERSHELL)
# ==============================================================================

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " [CHẠY JOB] TỔNG HỢP BẢNG GOLD TRÊN SPARK MASTER" -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Cyan
docker exec spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 2G `
  --executor-memory 2G `
  --executor-cores 2 `
  /opt/spark-apps/src/jobs/silver_to_gold_job.py

Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host " [KIỂM TRA HDFS] BẢNG 1: gold.xu_huong_ngay" -ForegroundColor Yellow
Write-Host "==================================================================" -ForegroundColor Cyan
docker exec namenode hdfs dfs -ls /lakehouse/bd05_tin_tuc/gold/xu_huong_ngay.parquet

Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host " [KIỂM TRA HDFS] BẢNG 2: gold.canh_bao_dot_bien" -ForegroundColor Yellow
Write-Host "==================================================================" -ForegroundColor Cyan
docker exec namenode hdfs dfs -ls /lakehouse/bd05_tin_tuc/gold/canh_bao_dot_bien.parquet

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " HOÀN TẤT TỔNG HỢP TẦNG GOLD TRÊN LAKEHOUSE!" -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Cyan
