# src/database/db_manager.py
# Quản lý kết nối và thao tác với cơ sở dữ liệu PostgreSQL

import psycopg2
from src.core.config import DB_CONFIG

class DBManager:
    @staticmethod
    def get_connection():
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except Exception as e:
            print(f"❌ DB Error: {e}")
            return None

    @staticmethod
    def init_db():
        """Chạy lệnh tạo bảng"""
        conn = DBManager.get_connection()
        if conn:
            cur = conn.cursor()
            # Copy nội dung file schema.sql vào đây hoặc đọc file
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fraud_logs (
                    id SERIAL PRIMARY KEY,
                    amount FLOAT,
                    anomaly_score FLOAT,
                    is_predicted_fraud BOOLEAN,
                    model_version VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            conn.close()
            print("✅ Database initialized!")

if __name__ == "__main__":
    DBManager.init_db()