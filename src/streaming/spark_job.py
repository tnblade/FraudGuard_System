# src/streaming/spark_job.py
# Spark Structured Streaming Job để đọc dữ liệu giao dịch từ Kafka,

import sys
import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pandas as pd
from kafka import KafkaProducer

# Import module dự báo của chúng ta
# Thêm đường dẫn root vào path để Spark tìm thấy src
sys.path.append(os.getcwd())
from src.ml.predictor import FraudPredictor
from src.core.config import KAFKA_BOOTSTRAP_SERVERS, JDBC_DRIVER_PATH

# Cấu hình Kafka Topic
INPUT_TOPIC = "raw_transactions"
OUTPUT_TOPIC_PREDICTIONS = "fraud_predictions"
OUTPUT_TOPIC_ALERTS = "fraud_alerts"

# Khởi tạo Global Predictor (Load model 1 lần duy nhất)
# Lưu ý: Trong môi trường Spark Cluster thật, nên dùng mapPartitions. 
# Với Demo single node, global var là ổn.
predictor = None

def get_predictor():
    global predictor
    if predictor is None:
        print("Lazy loading Predictor...")
        predictor = FraudPredictor()
    return predictor

def write_to_kafka(topic, data):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for record in data:
            producer.send(topic, record)
        producer.flush()
        producer.close()
    except Exception as e:
        print(f"❌ Kafka Write Error: {e}")

def process_batch(df_batch, epoch_id):
    """
    Hàm xử lý từng lô dữ liệu (Batch) từ Kafka
    """
    if df_batch.count() == 0:
        return

    start_time = time.time()
    
    # 1. Chuyển Spark DataFrame -> Pandas DataFrame để đưa vào Model AI
    # (Vì TensorFlow chạy trên Pandas/Numpy tốt hơn)
    rows = df_batch.collect()
    pdf = pd.DataFrame(rows, columns=df_batch.columns)
    
    print(f"📦 Batch {epoch_id}: Đang xử lý {len(pdf)} giao dịch...")

    # 2. Gọi Model dự báo
    model = get_predictor()
    try:
        # Hàm predict trả về DF có thêm cột 'anomaly_score' và 'is_fraud_prediction'
        result_df = model.predict(pdf)
        
        # Thêm timestamp xử lý
        result_df['processed_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # 3. Gửi KẾT QUẢ DỰ BÁO vào topic 'fraud_predictions' (Để Dashboard vẽ biểu đồ)
        records = result_df.to_dict(orient='records')
        write_to_kafka(OUTPUT_TOPIC_PREDICTIONS, records)
        
        # 4. Lọc và gửi CẢNH BÁO vào topic 'fraud_alerts'
        # Chỉ lấy những cái được model dự đoán là Fraud (True)
        fraud_alerts = result_df[result_df['is_fraud_prediction'] == True]
        
        if not fraud_alerts.empty:
            alert_records = fraud_alerts.to_dict(orient='records')
            write_to_kafka(OUTPUT_TOPIC_ALERTS, alert_records)
            print(f"🚨 PHÁT HIỆN {len(fraud_alerts)} GIAO DỊCH GIAN LẬN! Đã gửi cảnh báo.")
            
            # (Tùy chọn) In ra màn hình console vài dòng để debug
            print(fraud_alerts[['amount', 'anomaly_score']].head())

    except Exception as e:
        print(f"❌ Lỗi trong quá trình dự báo: {e}")
        import traceback
        traceback.print_exc()

    print(f"✅ Hoàn thành Batch {epoch_id} trong {time.time() - start_time:.2f}s")

def start_streaming():
    print("🚀 Đang khởi động Spark Streaming Job...")
    
    # Cấu hình Spark với gói Kafka
    spark = SparkSession.builder \
        .appName("FraudDetectorSystem") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Schema đúng với dữ liệu Paysim1
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

    # Đọc dữ liệu từ Kafka 'raw_transactions'
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS[0]) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON từ Kafka value
    df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Bắt đầu luồng xử lý
    query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='2 seconds') \
        .start()

    print(f"📡 Đang lắng nghe topic '{INPUT_TOPIC}'...")
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()