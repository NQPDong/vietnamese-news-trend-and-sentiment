import json
import time
import pandas as pd
from kafka import KafkaProducer


def get_kafka_producer(bootstrap_servers='localhost:9094'):
    """
    Khởi tạo Kafka Producer kết nối tới broker.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version=(2, 5, 0),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )


def stream_news_events(parquet_path='data/filtered_2021_2022/news_2021_2022.parquet',
                       topic='news-topic',
                       max_events=20,
                       delay_seconds=0.5,
                       bootstrap_servers='localhost:9094'):
    """
    Giả lập phát lại tin tức từ dataset vào Kafka Topic.
    """
    print(" KAFKA STREAMING INGESTION ")
    print(f" Kafka Broker: {bootstrap_servers}")
    print(f" Target Topic: {topic}")
    print(f" Số lượng sự kiện phát: {max_events} bài")

    producer = get_kafka_producer(bootstrap_servers)

    print(f"\n[1/3] Đang nạp mẫu tin tức từ: {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    sample_df = df.head(max_events).copy()

    print(f"[2/3] Bắt đầu truyền dòng sự kiện tin tức vào Kafka Topic '{topic}'...")
    for idx, row in sample_df.iterrows():
        event = {
            "id": int(row["id"]),
            "title": str(row["title"]),
            "content": str(row["content"])[:300] + "...",
            "source": str(row["source"]),
            "topic": str(row["topic"]),
            "crawled_at": str(row["crawled_at"])
        }

        producer.send(topic, value=event)
        print(f" -> [Đã phát #{idx+1:02d}] Nguồn: {event['source']} | Tiêu đề: {event['title'][:50]}...")
        time.sleep(delay_seconds)

    producer.flush()
    print(f"\n[3/3] Hoàn tất phát thành công {max_events} sự kiện tin tức vào Kafka!")
    print("=" * 70)


if __name__ == "__main__":
    stream_news_events()
