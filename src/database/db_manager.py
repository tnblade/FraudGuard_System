# src/database/db_manager.py
# Quản lý kết nối và thao tác với cơ sở dữ liệu PostgreSQL

import psycopg2
from src.core.config import DB_CONFIG

class DBManager:
    @staticmethod
    def get_connection():
        try:
            return psycopg2.connect(**DB_CONFIG)
        except Exception as e:
            print(f"❌ DB Connect Error: {e}")
            return None

    @staticmethod
    def init_db():
        print("🐘 Initializing Database...")
        # 1. Tạo DB (Kết nối vào postgres mặc định)
        try:
            conn = psycopg2.connect(dbname="postgres", user="postgres", host="localhost")
            conn.autocommit = True
            cur = conn.cursor()
            try: cur.execute("CREATE USER kaggle WITH PASSWORD 'bigdata';")
            except: pass
            try: cur.execute("CREATE DATABASE fraud_db OWNER kaggle;")
            except: pass
            cur.close(); conn.close()
        except Exception as e:
            print(f"⚠️ User/DB init warning: {e}")

        # 2. Tạo Bảng
        try:
            conn = DBManager.get_connection()
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fraud_alerts (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    amount FLOAT,
                    anomaly_score FLOAT,
                    type VARCHAR(50),
                    is_checked BOOLEAN DEFAULT FALSE
                );
            """)
            cur.close(); conn.close()
            print("✅ Database Ready!")
        except Exception as e:
            print(f"❌ Table Init Error: {e}")

    @staticmethod
    def insert_alert(amount, score, tx_type):
        conn = DBManager.get_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO fraud_alerts (amount, anomaly_score, type) VALUES (%s, %s, %s)",
                    (float(amount), float(score), str(tx_type))
                )
                conn.commit()
                cur.close(); conn.close()
            except Exception as e:
                print(f"❌ Insert Error: {e}")