# ==============================================================================
# KHỞI CHẠY GIAO DIỆN STREAMLIT DASHBOARD
# ==============================================================================

Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host "   KHỞI ĐỘNG GIAO DIỆN DASHBOARD THEO DÕI XU HƯỚNG & CẢM XÚC TIN TỨC      " -ForegroundColor Yellow
Write-Host "==========================================================================" -ForegroundColor Cyan

$streamlitPath = ".venv\Scripts\streamlit.exe"

if (-not (Test-Path $streamlitPath)) {
    Write-Host "Chưa tìm thấy Streamlit trong .venv. Đang cài đặt..." -ForegroundColor Yellow
    .venv\Scripts\pip install streamlit plotly
}

Write-Host "`nĐang mở giao diện tại: http://localhost:8501" -ForegroundColor Green
& $streamlitPath run src/dashboard/app.py --server.port 8501 --server.headless false
