import os
import subprocess
import time
from configs import KAFKA_TOPIC

def main():
    print("🚀 BẮT ĐẦU CHẠY HỆ THỐNG PHÂN TÁN...")
    
    # 1. Setup Environment (Chạy file sh)
    # Lưu ý: Trên Kaggle cần cấp quyền chmod +x setup_kaggle.sh trước
    subprocess.run(["bash", "setup_kaggle.sh"])
    
    # 2. Setup Variables
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    # Update path tùy vào thư mục giải nén
    
    # 3. Start Kafka & Zookeeper (Background)
    print("🐘 Starting Kafka...")
    # (Viết code subprocess gọi bin/kafka-server-start.sh ở đây - như code cũ)
    # Vì file main.py sẽ rất dài, bạn có thể copy logic từ Cell 2 cũ vào đây
    
    # 4. Init DB
    import database
    database.init_db()
    
    # 5. Train Model (Nếu chưa có)
    import train_model
    if not os.path.exists("fraud_detection_model.h5"):
        train_model.train()
        
    # 6. Start Components
    # Chạy các process song song
    print("🔥 Starting Spark Processor...")
    subprocess.Popen(["python", "spark_processor.py"])
    
    print("🌍 Starting Backend API...")
    subprocess.Popen(["python", "api_server.py"])
    
    print("📊 Starting Dashboard...")
    subprocess.Popen(["streamlit", "run", "dashboard.py"])
    
    print("✅ HỆ THỐNG ĐÃ CHẠY! Kiểm tra API URL và Localtunnel URL trong log.")
    
    # Giữ main thread sống
    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()