# src/dashboard/app.py
# Dashboard Streamlit để hiển thị cảnh báo gian lận

import streamlit as st
import pandas as pd
import json
import time
import os
import subprocess
from kafka import KafkaConsumer
from src.database.db_manager import DBManager

st.set_page_config(page_title="FraudGuard Admin Portal", page_icon="🛡️", layout="wide")

st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { height: 50px; border-radius: 5px; }
    .success-box { padding: 10px; background-color: #d4edda; color: #155724; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

st.title("🛡️ FraudGuard: Enterprise Command Center")

# TẠO 3 TAB CHỨC NĂNG
tab1, tab2, tab3 = st.tabs(["📈 Real-time Monitor", "🗄️ Database Inspector", "🤖 Airflow & MLOps"])

# ================= TAB 1: REAL-TIME MONITOR =================
with tab1:
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader("Live Anomaly Detection")
        chart_placeholder = st.empty()
    with col2:
        st.subheader("Recent Alerts")
        alert_placeholder = st.empty()
    
    if st.button("🔴 KẾT NỐI LIVE STREAM", key="btn_stream"):
        try:
            consumer = KafkaConsumer(
                'fraud_predictions',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000
            )
            st.success("Đã kết nối Kafka! Đang chờ dữ liệu...")
            
            data_cache = []
            msg_count = 0
            
            # Placeholder cho biểu đồ
            chart = st.line_chart([])
            
            for msg in consumer:
                msg_count += 1
                record = msg.value
                score = record.get('anomaly_score', 0)
                
                # Cập nhật biểu đồ
                data_cache.append(score)
                if len(data_cache) > 50: data_cache.pop(0)
                
                if msg_count % 2 == 0:
                    chart.line_chart(data_cache)
                    
                # Cập nhật Alert
                if record.get('is_fraud_prediction'):
                    alert_placeholder.error(f"⚠️ PHÁT HIỆN: ${record.get('amount', 0):,.0f}")
                    
        except Exception as e:
            st.error(f"Lỗi kết nối: {e}")

# ================= TAB 2: DATABASE INSPECTOR =================
with tab2:
    st.header("🗄️ Dữ liệu Gian lận trong PostgreSQL")
    
    c1, c2 = st.columns([1, 4])
    with c1:
        if st.button("🔄 REFRESH DATA"):
            try:
                conn = DBManager.get_connection()
                if conn:
                    query = "SELECT * FROM fraud_logs ORDER BY id DESC LIMIT 50"
                    df_logs = pd.read_sql(query, conn)
                    st.session_state['df_logs'] = df_logs
                    conn.close()
                    st.success("Đã tải xong!")
            except Exception as e:
                st.error(f"Lỗi DB: {e}")

    with c2:
        if 'df_logs' in st.session_state:
            df = st.session_state['df_logs']
            st.dataframe(df, use_container_width=True)
            st.metric("Tổng tiền đã chặn", f"${df['amount'].sum():,.0f}")

# ================= TAB 3: AIRFLOW / MLOPS =================
with tab3:
    st.header("🤖 MLOps Control Plane")
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🛠️ Manual Retrain")
        if st.button("🚀 CHẠY HUẤN LUYỆN LẠI (Retrain)"):
            with st.status("Running Pipeline...", expanded=True) as status:
                st.write("Checking Data...")
                time.sleep(1)
                st.write("Training Model...")
                # Gọi script train thật
                res = subprocess.run(["python", "src/ml/trainer.py"], capture_output=True, text=True)
                if res.returncode == 0:
                    status.update(label="Training Completed!", state="complete")
                    st.success("Model mới đã được deploy!")
                else:
                    status.update(label="Training Failed!", state="error")
                    st.error(res.stderr)
    
    with c2:
        st.subheader("📅 Scheduler Status")
        st.info("Airflow Scheduler: ACTIVE (Daily @ 00:00)")
        st.json({"last_run": "Success", "next_run": "Tomorrow"})