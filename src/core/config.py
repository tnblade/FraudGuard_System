# src/core/config.py
# Cấu hình chung cho toàn bộ hệ thống Fraud Detection
# Bao gồm đường dẫn, cấu hình Kafka, Database, và các hằng số khác
import os

# Đường dẫn gốc của Project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Đường dẫn Model & Scaler
MODEL_PATH = os.path.join(BASE_DIR, "models", "fraud_detection_model.h5")
SCALER_PATH = os.path.join(BASE_DIR, "models", "scaler.pkl")

# Các cấu hình khác (Database, Kafka...) giữ nguyên hoặc thêm vào sau
DB_CONFIG = {
    "dbname": "fraud_db",
    "user": "kaggle",
    "password": "bigdata",
    "host": "localhost",
    "port": "5432"
}