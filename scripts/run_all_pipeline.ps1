# ==============================================================================
# PIPELINE ĐIỀU PHỐI TOÀN BỘ HỆ THỐNG LAKEHOUSE (END-TO-END MASTER ORCHESTRATOR)
# Đồ án: Theo dõi Xu hướng và Cảm xúc Tin tức Tiếng Việt
# ==============================================================================

Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host "   BẮT ĐẦU CHẠY TOÀN BỘ PIPELINE BIG DATA LAKEHOUSE (1-CLICK RUNNER)      " -ForegroundColor Yellow
Write-Host "==========================================================================" -ForegroundColor Cyan

# 1. KIỂM TRA TRẠNG THÁI CONTAINER
Write-Host "`n[Bước 1/5] Kiểm tra hạ tầng Docker HDFS & Spark..." -ForegroundColor Green
$runningContainers = docker ps --format "{{.Names}}"
if ($runningContainers -notmatch "namenode" -or $runningContainers -notmatch "spark-master") {
    Write-Host "Hạ tầng chưa chạy. Đang kích hoạt docker compose up -d..." -ForegroundColor Yellow
    docker compose up -d
    Start-Sleep -Seconds 5
}
Write-Host "✓ Cụm HDFS (3 DataNodes) và Apache Spark đã sẵn sàng!" -ForegroundColor Green

# 2. CHẠY JOB 1: BRONZE -> SILVER (LÀM SẠCH & KHỬ TRÙNG LẶP)
Write-Host "`n[Bước 2/5] Đang chạy Job 1: Làm sạch & Khử trùng lặp (Bronze -> Silver)..." -ForegroundColor Green
docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" `
    /opt/spark-apps/src/jobs/bronze_to_silver_job.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "Lỗi xảy ra tại Job 1. Dừng pipeline!" -ForegroundColor Red
    exit 1
}

# 3. CHẠY JOB 2: GÁN NHÃN CẢM XÚC BẰNG SPARK MLLIB
Write-Host "`n[Bước 3/5] Đang chạy Job 2: Gán nhãn cảm xúc bằng mô hình Spark MLlib..." -ForegroundColor Green
docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" `
    /opt/spark-apps/src/jobs/silver_sentiment_inference.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "Lỗi xảy ra tại Job 2. Dừng pipeline!" -ForegroundColor Red
    exit 1
}

# 4. CHẠY JOB 3: TỔNG HỢP TẦNG GOLD & PHÁT HIỆN ĐỘT BIẾN Z-SCORE
Write-Host "`n[Bước 4/5] Đang chạy Job 3: Tổng hợp bảng Gold & Tính điểm đột biến Z-score..." -ForegroundColor Green
docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" `
    /opt/spark-apps/src/jobs/silver_to_gold_job.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "Lỗi xảy ra tại Job 3. Dừng pipeline!" -ForegroundColor Red
    exit 1
}

# 5. ĐỒNG BỘ DỮ LIỆU GOLD PHỤC VỤ DASHBOARD
Write-Host "`n[Bước 5/5] Đang đồng bộ bảng Gold từ HDFS sang tầng phục vụ Dashboard..." -ForegroundColor Green
docker exec namenode rm -rf /tmp/gold
docker exec namenode hdfs dfs -get /lakehouse/bd05_tin_tuc/gold /tmp/gold
if (-not (Test-Path "data/lakehouse/gold")) { New-Item -ItemType Directory -Path "data/lakehouse/gold" -Force }
docker cp namenode:/tmp/gold/. data/lakehouse/gold/

Write-Host "`n==========================================================================" -ForegroundColor Cyan
Write-Host "✓ TOÀN BỘ PIPELINE BIG DATA ĐÃ CHẠY HOÀN TẤT THÀNH CÔNG 100%!" -ForegroundColor Green
Write-Host "✓ Để khởi chạy giao diện Dashboard, hãy chạy lệnh:" -ForegroundColor Yellow
Write-Host "   powershell -ExecutionPolicy Bypass -File scripts/run_dashboard.ps1" -ForegroundColor White
Write-Host "==========================================================================" -ForegroundColor Cyan
