

# configs.py
import os

# --- CẤU HÌNH NGROK ---
NGROK_AUTH_TOKEN = "NHAP_TOKEN_NGROK_CUA_BAN_VAO_DAY" 

# --- CẤU HÌNH DATABASE ---
DB_CONFIG = {
    "dbname": "fraud_db",
    "user": "kaggle",
    "password": "bigdata",
    "host": "localhost",
    "port": "5432"
}

# --- PATHS ---
MODEL_PATH = "fraud_detection_model.h5"
SCALER_PATH = "scaler.pkl"
JDBC_DRIVER_PATH = "postgresql-42.6.0.jar"

# --- KAFKA ---
KAFKA_TOPIC = "fraud_detection"
KAFKA_SERVER = "localhost:9092"