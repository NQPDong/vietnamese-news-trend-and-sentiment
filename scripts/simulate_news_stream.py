import os
import time
import random
from datetime import datetime

# Danh sách mẫu dữ liệu giả lập tin tức tiếng Việt
SAMPLE_TOPICS = ["Thời sự", "Kinh doanh", "Thể thao", "Công nghệ", "Giải trí", "Y tế", "Giáo dục"]
SAMPLE_SOURCES = ["vnexpress", "dantri", "tuoitre", "thanhnien", "zingnews", "24h.com.vn"]
SAMPLE_AUTHORS = ["Nguyễn Văn A", "Trần Thị B", "Lê Hoàng C", "Phạm Minh D", "Ban Thời Sự"]

TITLES_POOL = [
    "Việt Nam đẩy mạnh chuyển đổi số trong các doanh nghiệp vừa và nhỏ",
    "Thị trường tài chính ghi nhận dấu hiệu phục hồi tích cực sau các chính sách mới",
    "Đội tuyển bóng đá quốc gia bắt đầu đợt tập trung chuẩn bị cho giải đấu lớn",
    "Phát hiện lỗ hổng bảo mật nghiêm trọng trong ứng dụng phổ biến",
    "Giá xăng dầu thế giới biến động mạnh ảnh hưởng đến thị trường trong nước",
    "Bộ Giáo dục công bố phương án thi tốt nghiệp THPT và tuyển sinh mới",
    "Hạ tầng giao thông trọng điểm gấp rút hoàn thành tiến độ trước hạn",
    "Nhiều dự án công nghệ xanh nhận được nguồn vốn đầu tư quốc tế lớn"
]

CONTENTS_POOL = [
    "Sáng nay tại Hà Nội, hội nghị về giải pháp thúc đẩy kinh tế số và trí tuệ nhân tạo đã diễn ra với sự tham gia của hơn 500 chuyên gia hàng đầu. Các đại biểu thảo luận về cơ chế chính sách, nguồn nhân lực và ứng dụng dữ liệu lớn trong tối ưu hóa vận hành.",
    "Báo cáo mới nhất từ cơ quan thống kê cho thấy chỉ số niềm tin người tiêu dùng đang tăng trưởng ổn định. Các doanh nghiệp trong nước liên tục mở rộng sản xuất và tuyển dụng lao động trong bối cảnh chuỗi cung ứng toàn cầu dần phục hồi.",
    "Huấn luyện viên trưởng đã công bố danh sách triệu tập đợt này với nhiều gương mặt trẻ triển vọng. Toàn đội đặt quyết tâm cao hướng tới mục tiêu giành thứ hạng cao nhất tại giải vô địch khu vực.",
    "Các chuyên gia an ninh mạng khuyến cáo người dùng cần cập nhật phiên bản mới nhất ngay lập tức để tránh nguy cơ rò rỉ thông tin cá nhân. Cơ quan chức năng đang phối hợp để điều tra nguồn gốc mã độc."
]


def generate_simulated_article(article_id: int) -> dict:
    now = datetime.now()
    topic = random.choice(SAMPLE_TOPICS)
    source = random.choice(SAMPLE_SOURCES)
    title = random.choice(TITLES_POOL) + f" (Bản tin số {article_id})"
    content = random.choice(CONTENTS_POOL)
    
    return {
        "id": article_id,
        "author": random.choice(SAMPLE_AUTHORS),
        "content": content,
        "picture_count": random.randint(1, 5),
        "processed": 1,
        "source": source,
        "title": title,
        "topic": topic,
        "url": f"https://{source}.vn/tin-tuc/{now.strftime('%Y%m%d')}/bai-viet-{article_id}.html",
        "crawled_at": now.strftime("%Y-%m-%d %H:%M:%S")
    }


def main():
    print("=" * 65)
    print("   SCRIPT GIẢ LẬP LUỒNG DỮ LIỆU TIN TỨC MỚI (STREAM SIMULATOR)")
    print("   Mục tiêu: Đáp ứng yêu cầu thầy về sinh luồng dữ liệu thời gian thực")
    print("=" * 65)
    
    output_dir = os.path.join("data", "stream_simulation")
    os.makedirs(output_dir, exist_ok=True)
    
    num_articles = 10
    interval_seconds = 1.5
    
    print(f"\n[Bắt đầu] Giả lập sinh {num_articles} bài báo mới liên tục (mỗi {interval_seconds}s/bài)...")
    
    simulated_records = []
    base_id = 900000 + random.randint(1000, 9999)
    
    for i in range(1, num_articles + 1):
        article = generate_simulated_article(base_id + i)
        simulated_records.append(article)
        
        print(f"[{i:02d}/{num_articles}] [{article['crawled_at']}] [{article['source'].upper()}] - {article['title']}")
        print(f"      -> Chuyên mục: {article['topic']} | URL: {article['url']}")
        time.sleep(interval_seconds)
        
    # Lưu file micro-batch mô phỏng
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"news_microbatch_{timestamp_str}.json")
    
    import json
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(simulated_records, f, ensure_ascii=False, indent=2)
        
    print("\n" + "=" * 65)
    print(f"✓ Đã sinh thành công {num_articles} bản ghi dữ liệu mới!")
    print(f"✓ Đã lưu file micro-batch tại: {output_file}")
    print("  Luồng này sẵn sàng để Kafka Producer hoặc Spark Streaming tiếp nhận.")
    print("=" * 65)


if __name__ == "__main__":
    main()
