import json
import os
import subprocess
import tempfile
import pandas as pd


def consume_and_save_to_hdfs(topic='news-topic',
                             max_messages=10,
                             hdfs_target_dir='/lakehouse/bd05_tin_tuc/bronze/streaming'):
    """
    Nhận các tin tức từ Kafka Topic thông qua Kafka Container và đẩy trực tiếp vào HDFS Bronze .
    """
    print(" KAFKA STREAMING CONSUMER -> HDFS BRONZE")
    print(f" Lắng nghe Topic : {topic}")
    print(f" Kafka Container : kafka (cổng 9092 nội bộ)")

    print(f"\n[1/3] Đang nhận tối đa {max_messages} sự kiện từ Kafka Topic...")
    cmd = [
        "docker", "exec", "kafka",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "localhost:9092",
        "--topic", topic,
        "--from-beginning",
        "--max-messages", str(max_messages),
        "--timeout-ms", "8000"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    lines = [line.strip() for line in result.stdout.strip().split("\n") if line.strip().startswith("{")]

    messages = []
    for line in lines:
        try:
            item = json.loads(line)
            messages.append(item)
            print(f" <- [Đã nhận #{len(messages):02d}] Nguồn: {item.get('source')} | Tiêu đề: {item.get('title')[:40]}...")
        except json.JSONDecodeError:
            continue

    if not messages:
        print("-> Không tìm thấy bản ghi mới trong Kafka topic.")
        return

    print(f"\n[2/3] Đang đóng gói {len(messages)} bản ghi thành định dạng Parquet...")
    df = pd.DataFrame(messages)
    temp_dir = tempfile.mkdtemp()
    temp_parquet = os.path.join(temp_dir, "stream_batch.parquet")
    df.to_parquet(temp_parquet, engine="pyarrow", index=False)

    print(f"[3/3] Đang truyền luồng dữ liệu vào cụm phân tán HDFS ({hdfs_target_dir})...")
    # Tạo thư mục trên HDFS
    subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_target_dir], check=True)
    # Copy file vào container NameNode
    subprocess.run(["docker", "cp", temp_parquet, "namenode:/tmp/stream_batch.parquet"], check=True)
    # Đẩy lên HDFS Bronze
    subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", "/tmp/stream_batch.parquet", f"{hdfs_target_dir}/"], check=True)

    print(f"\n✓ THÀNH CÔNG: Đã ghi nhận trọn vẹn luồng Streaming vào HDFS!")
    print(f"  Vị trí HDFS: {hdfs_target_dir}/stream_batch.parquet")
    print("=" * 70)


if __name__ == "__main__":
    consume_and_save_to_hdfs()
