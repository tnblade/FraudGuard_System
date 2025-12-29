# src/streaming/spark_job.py
# Ứng dụng Spark Streaming để phát hiện gian lận thời gian thực

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType
import tensorflow as tf
import pandas as pd
import numpy as np
import pickle
from src.core.config import MODEL_PATH, SCALER_PATH, JDBC_DRIVER_PATH, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from src.database.db_manager import DBManager

def process_batch(df_batch, epoch_id):
    if df_batch.count() == 0: return
    
    # Logic: Spark -> Pandas -> AI Model -> Database
    rows = df_batch.collect()
    pdf = pd.DataFrame(rows, columns=df_batch.columns)
    
    # (Load Model & Scaler inside worker logic or broadcast variable)
    # Để code ngắn gọn, giả lập logic dự đoán
    # Thực tế bạn cần load model 1 lần (Global) để tối ưu
    
    # Giả lập phát hiện gian lận
    for _, row in pdf.iterrows():
        # Gọi hàm DBManager để lưu
        if row['amount'] > 500000: # Ví dụ logic đơn giản
             DBManager.insert_alert(row['amount'], 99.9, "TRANSFER")
             print(f"⚠️ Alert saved: {row['amount']}")

def start_streaming():
    spark = SparkSession.builder \
        .appName("FraudDetector") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.jars", JDBC_DRIVER_PATH) \
        .getOrCreate()
    
    # Schema Definition
    schema = StructType([
        StructField("amount", DoubleType(), True)
        # ... Thêm các field khác ...
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS[0]) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()
    
    query = df.writeStream.foreachBatch(process_batch).start()
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()