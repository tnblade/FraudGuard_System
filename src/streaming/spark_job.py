# src/streaming/spark_job.py
# Spark Streaming Job để đọc giao dịch từ Kafka, dự báo gian lận và xử lý cảnh báo



import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pandas as pd

sys.path.append(os.getcwd())

from src.ml.predictor import FraudPredictor
from src.core.config import KAFKA_BOOTSTRAP_SERVERS
from src.services.alert_service import AlertService # <--- Class mới

# Cấu hình Kafka Input
INPUT_TOPIC = "raw_transactions"

# Khởi tạo Global Services (Lazy Loading)
predictor = None
alert_service = None

def get_services():
    global predictor, alert_service
    if predictor is None:
        print("🛠️ Initializing Services...")
        predictor = FraudPredictor()
        alert_service = AlertService()
    return predictor, alert_service

def process_batch(df_batch, epoch_id):
    if df_batch.count() == 0: return

    start_time = time.time()
    
    # 1. Chuyển đổi dữ liệu
    rows = df_batch.collect()
    pdf = pd.DataFrame(rows, columns=df_batch.columns)
    
    # 2. Lấy Services
    model, alerter = get_services()
    
    try:
        # 3. AI Dự báo
        print(f"📦 Batch {epoch_id}: Predicting {len(pdf)} tx...")
        result_df = model.predict(pdf)
        
        # 4. Giao việc xử lý kết quả cho AlertService
        # (Spark Job không cần biết AlertService làm gì bên trong)
        alerter.process_alerts(result_df)

    except Exception as e:
        print(f"❌ Batch Processing Error: {e}")
        import traceback
        traceback.print_exc()

    print(f"✅ Batch {epoch_id} done in {time.time() - start_time:.2f}s")

def start_streaming():
    print("🚀 Starting Spark Streaming (Clean Architecture)...")
    
    spark = SparkSession.builder \
        .appName("FraudGuard_Pro") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Schema
    schema = StructType([
        StructField("step", IntegerType()),
        StructField("type", StringType()),
        StructField("amount", DoubleType()),
        StructField("nameOrig", StringType()),
        StructField("oldbalanceOrg", DoubleType()),
        StructField("newbalanceOrig", DoubleType()),
        StructField("nameDest", StringType()),
        StructField("oldbalanceDest", DoubleType()),
        StructField("newbalanceDest", DoubleType()),
        StructField("isFraud", IntegerType()),
        StructField("isFlaggedFraud", IntegerType())
    ])

    # Read Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS[0]) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='2 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()