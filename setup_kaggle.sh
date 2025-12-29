#!/bin/bash
echo "🚀 Đang thiết lập môi trường Big Data..."

# 1. Update & Cài đặt Packages
apt-get update > /dev/null
apt-get install -y openjdk-8-jdk-headless postgresql postgresql-contrib > /dev/null

# 2. Cài Python Libs
pip install -r requirements.txt

# 3. Tải Spark & Kafka
wget -q https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
tar xf spark-3.4.0-bin-hadoop3.tgz
wget -q https://archive.apache.org/dist/kafka/3.4.0/kafka_2.12-3.4.0.tgz
tar xf kafka_2.12-3.4.0.tgz

# 4. Tải JDBC Driver
wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# 5. Khởi động Postgres
service postgresql start

echo "✅ SETUP COMPLETED!"