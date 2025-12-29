# src/services/alert_service.py
# Dịch vụ cảnh báo gian lận: Gửi cảnh báo qua Kafka và lưu vào Database

import json
import pandas as pd
from kafka import KafkaProducer
from src.database.db_manager import DBManager
from src.core.config import KAFKA_BOOTSTRAP_SERVERS

class AlertService:
    def __init__(self):
        # Khởi tạo Kafka Producer 1 lần dùng mãi mãi
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_to_kafka(self, topic, data_list):
        """Gửi danh sách record vào Kafka"""
        try:
            for record in data_list:
                self.producer.send(topic, record)
            self.producer.flush()
        except Exception as e:
            print(f"❌ Kafka Error: {e}")

    def save_to_database(self, df_frauds):
        """Lưu danh sách gian lận vào Database"""
        if df_frauds.empty: return

        conn = DBManager.get_connection()
        if not conn:
            print("⚠️ DB Connection Failed!")
            return

        try:
            cursor = conn.cursor()
            query = """
                INSERT INTO fraud_logs (amount, anomaly_score, is_predicted_fraud, created_at)
                VALUES (%s, %s, %s, NOW())
            """
            
            data_tuples = []
            for _, row in df_frauds.iterrows():
                data_tuples.append((
                    float(row['amount']),
                    float(row['anomaly_score']),
                    bool(row['is_fraud_prediction'])
                ))
            
            cursor.executemany(query, data_tuples)
            conn.commit()
            cursor.close()
            conn.close()
            print(f"💾 Đã lưu {len(data_tuples)} cảnh báo vào DB.")
        except Exception as e:
            print(f"❌ DB Write Error: {e}")

    def process_alerts(self, result_df):
        """
        Hàm Wrapper: Nhận kết quả dự báo -> Tự động phân luồng Kafka & DB
        """
        # 1. Gửi TOÀN BỘ kết quả ra Kafka (cho Dashboard)
        full_records = result_df.to_dict(orient='records')
        self.send_to_kafka("fraud_predictions", full_records)

        # 2. Lọc Fraud
        fraud_df = result_df[result_df['is_fraud_prediction'] == True]
        
        if not fraud_df.empty:
            print(f"🚨 PHÁT HIỆN {len(fraud_df)} GIAN LẬN!")
            
            # a. Gửi Alert Kafka
            alert_records = fraud_df.to_dict(orient='records')
            self.send_to_kafka("fraud_alerts", alert_records)
            
            # b. Lưu DB
            self.save_to_database(fraud_df)