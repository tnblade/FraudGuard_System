# spark_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import tensorflow as tf
import pandas as pd
import numpy as np
import pickle
import database
from configs import MODEL_PATH, SCALER_PATH, JDBC_DRIVER_PATH, KAFKA_TOPIC

def run_spark_job():
    # 1. Init Spark
    spark = SparkSession.builder \
        .appName("FraudDetectionSystem") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.jars", JDBC_DRIVER_PATH) \
        .config("spark.driver.extraClassPath", JDBC_DRIVER_PATH) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Load Resources
    print("⏳ Loading AI Engine...")
    model = tf.keras.models.load_model(MODEL_PATH)
    with open(SCALER_PATH, 'rb') as f:
        scaler = pickle.load(f)
        
    # Lấy thứ tự cột chuẩn
    if hasattr(scaler, "feature_names_in_"):
        required_cols = list(scaler.feature_names_in_)
    else:
        required_cols = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest',
                         'type_CASH_IN', 'type_CASH_OUT', 'type_DEBIT', 'type_PAYMENT', 'type_TRANSFER']

    # 3. Processing Function
    def process_batch(df_batch, epoch_id):
        if df_batch.count() == 0: return
        
        # Convert to Pandas safely
        rows = df_batch.collect()
        pdf = pd.DataFrame(rows, columns=df_batch.columns)
        
        # Align columns & Scale
        pdf_input = pdf.reindex(columns=required_cols, fill_value=0)
        try:
            input_data = scaler.transform(pdf_input)
        except:
            return

        # Predict
        reconstructions = model.predict(input_data, verbose=0)
        mse = np.mean(np.power(input_data - reconstructions, 2), axis=1)
        pdf['anomaly_score'] = mse
        
        # Filter & Save to DB
        frauds = pdf[pdf['anomaly_score'] > 0.1]
        
        if not frauds.empty:
            print(f"⚠️ FOUND {len(frauds)} FRAUDS! Saving to DB...")
            for _, row in frauds.iterrows():
                # Lưu vào Postgres qua hàm util
                # Lấy type_TRANSFER nếu có, hoặc lấy type bất kỳ != 0
                tx_type = "UNKNOWN"
                for col_name in pdf.columns:
                    if "type_" in col_name and row[col_name] == 1:
                        tx_type = col_name
                        break
                
                database.save_alert(row['amount'], row['anomaly_score'], tx_type)

    # 4. Read Kafka
    schema = StructType([
        StructField("step", DoubleType(), True),
        StructField("type_CASH_IN", DoubleType(), True),
        StructField("type_CASH_OUT", DoubleType(), True),
        StructField("type_DEBIT", DoubleType(), True),
        StructField("type_PAYMENT", DoubleType(), True),
        StructField("type_TRANSFER", DoubleType(), True),
        StructField("amount", DoubleType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", DoubleType(), True) 
    ])

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
        
    df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    print("🚀 Spark Streaming is Running...")
    query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    run_spark_job()