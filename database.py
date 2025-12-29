# database.py
import psycopg2
from configs import DB_CONFIG

def get_connection():
    """Tạo kết nối tới PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")
        return None

def init_db():
    """Khởi tạo bảng dữ liệu nếu chưa có"""
    print("🐘 Initializing PostgreSQL Tables...")
    try:
        # Kết nối tới database gốc 'postgres' để tạo DB 'fraud_db'
        # Lưu ý: Trên Kaggle user mặc định là postgres
        conn_base = psycopg2.connect(dbname="postgres", user="postgres", host="localhost")
        conn_base.autocommit = True
        cur = conn_base.cursor()
        
        # Tạo User và DB (Bỏ qua lỗi nếu đã tồn tại)
        try: cur.execute("CREATE USER kaggle WITH PASSWORD 'bigdata';")
        except: pass
        try: cur.execute("CREATE DATABASE fraud_db OWNER kaggle;")
        except: pass
        
        cur.close()
        conn_base.close()

        # Kết nối vào fraud_db để tạo bảng
        conn = get_connection()
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
        cur.close()
        conn.close()
        print("✅ Database & Tables Ready!")
    except Exception as e:
        print(f"⚠️ Init DB Warning: {e}")

def save_alert(amount, score, tx_type):
    """Lưu 1 cảnh báo vào DB"""
    conn = get_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO fraud_alerts (amount, anomaly_score, type) VALUES (%s, %s, %s)",
                (float(amount), float(score), str(tx_type))
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"❌ Save Error: {e}")