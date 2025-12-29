# producer.py
# Kafka Producer để giả lập gửi dữ liệu giao dịch từ dataset Paysim

import time
import json
import pandas as pd
from kafka import KafkaProducer
import glob
import os

# Cấu hình Kafka
TOPIC = "raw_transactions"
BOOTSTRAP_SERVER = "localhost:9092"

def get_dataset():
    # Tìm file dataset Paysim trong Kaggle Input
    files = glob.glob("/kaggle/input/paysim1/*.csv") + glob.glob("data/*.csv")
    if not files: return None
    return files[0]

def start_producer():
    print(f"🔄 Đang kết nối Producer tới {BOOTSTRAP_SERVER}...")
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    file_path = get_dataset()
    if not file_path:
        print("❌ Không tìm thấy dataset để giả lập giao dịch!")
        return

    print(f"✅ Bắt đầu gửi dữ liệu từ: {file_path}")
    
    # Đọc file CSV (chunksize để không tràn RAM)
    # Giả lập: Gửi từng dòng một
    for chunk in pd.read_csv(file_path, chunksize=1000):
        for index, row in chunk.iterrows():
            # Tạo bản tin giao dịch
            transaction = row.to_dict()
            
            # Gửi vào Kafka
            producer.send(TOPIC, value=transaction)
            
            # Giả lập độ trễ (0.1 giây/giao dịch)
            time.sleep(0.1) 
            
            if index % 100 == 0:
                print(f"📤 Đã gửi {index} giao dịch...")

if __name__ == "__main__":
    # Đợi Kafka khởi động hẳn rồi mới chạy
    time.sleep(10)
    try:
        start_producer()
    except Exception as e:
        print(f"❌ Lỗi Producer: {e}")