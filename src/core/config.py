# src/core/config.py
# Cấu hình chung cho toàn bộ hệ thống Fraud Detection
# Bao gồm đường dẫn, cấu hình Kafka, Database, và các hằng số khác
# src/core/config.py


import os

# Đường dẫn gốc
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Đường dẫn Model
MODEL_DIR = os.path.join(BASE_DIR, "models")
MODEL_PATH = os.path.join(MODEL_DIR, "fraud_detection_model.h5")
SCALER_PATH = os.path.join(MODEL_DIR, "scaler.pkl")
COLUMNS_PATH = os.path.join(MODEL_DIR, "model_columns.pkl")

# Đường dẫn Driver JDBC (QUAN TRỌNG: Thêm dòng này để fix lỗi Import)
# Trên Kaggle, driver thường nằm ở root hoặc được tải về /kaggle/working
JDBC_DRIVER_PATH = os.path.join(BASE_DIR, "postgresql-42.6.0.jar")

# Cấu hình Database
DB_CONFIG = {
    "dbname": "fraud_db",
    "user": "kaggle",
    "password": "bigdata",
    "host": "localhost",
    "port": "5432"
}

# Cấu hình Kafka
KAFKA_TOPIC = "fraud_detection"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]