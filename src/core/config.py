# src/core/config.py
# Cấu hình chung cho toàn bộ hệ thống Fraud Detection
# Bao gồm đường dẫn, cấu hình Kafka, Database, và các hằng số khác
#        
import os
import getpass # Thư viện để nhập ẩn

# --- PATHS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODEL_PATH = os.path.join(BASE_DIR, "models", "fraud_detection_model.h5")
SCALER_PATH = os.path.join(BASE_DIR, "models", "scaler.pkl")
JDBC_DRIVER_PATH = "postgresql-42.6.0.jar" 

# --- KAFKA ---
KAFKA_TOPIC = "fraud_detection"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

# --- DATABASE ---
DB_CONFIG = {
    "dbname": "fraud_db",
    "user": "kaggle",
    "password": "bigdata",
    "host": "localhost",
    "port": "5432"
}

from kaggle_secrets import UserSecretsClient
user_secrets = UserSecretsClient()
NGROK_AUTH_TOKEN = user_secrets.get_secret("NGROK_TOKEN")

# Kiểm tra nhẹ xem đã nhập chưa
if not NGROK_AUTH_TOKEN:
    print("⚠️ Cảnh báo: Bạn chưa nhập Token!")
else:
    print("✅ Đã nhận Token thành công (đã ẩn).")