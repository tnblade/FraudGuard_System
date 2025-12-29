# src/workflows/retrain_dag.py
# DAG Airflow để tự động retrain mô hình phát hiện gian lận hàng ngày


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Import hàm train của mình
sys.path.append("/opt/airflow/dags/repo/FraudGuard_System") # Đường dẫn giả định
from src.ml.trainer import train_model

default_args = {
    'owner': 'tnblade',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fraud_model_retraining_daily',
    default_args=default_args,
    description='Pipeline tự động train lại model phát hiện gian lận',
    schedule_interval=timedelta(days=1), # Chạy mỗi ngày
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def check_new_data():
        print("🔍 Kiểm tra xem có đủ dữ liệu mới trong Postgres không...")
        # Logic: Query DB xem hôm nay có > 1000 giao dịch mới không
        return True

    task_check_data = PythonOperator(
        task_id='check_data_availability',
        python_callable=check_new_data
    )

    task_retrain = PythonOperator(
        task_id='retrain_model',
        python_callable=train_model # Hàm này sẽ load data từ DB và save .h5 mới
    )

    task_notify = PythonOperator(
        task_id='notify_admin',
        python_callable=lambda: print("📧 Gửi email báo cáo: Model đã update xong!")
    )

    # Định nghĩa luồng chạy
    task_check_data >> task_retrain >> task_notify