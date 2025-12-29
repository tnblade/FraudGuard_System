# src/dashboard/app.py
# Dashboard Streamlit để hiển thị cảnh báo gian lận

# src/dashboard/app.py
import sys
import os

# --- 1. SỬA LỖI IMPORT (QUAN TRỌNG) ---
# Thêm thư mục gốc vào đường dẫn để Python tìm thấy 'src'
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.append(root_dir)

# --- 2. CODE CHÍNH ---
import streamlit as st
import pandas as pd
import json
import time
import subprocess
from kafka import KafkaConsumer
# Bây giờ dòng này mới hoạt động:
from src.database.db_manager import DBManager

# --- CẤU HÌNH ---
st.set_page_config(page_title="FraudGuard Admin Portal", page_icon="🛡️", layout="wide")

# CSS Tùy chỉnh
st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { height: 50px; border-radius: 5px; }
    .metric-card { background-color: #f0f2f6; padding: 15px; border-radius: 10px; }
</style>
""", unsafe_allow_html=True)

st.title("🛡️ FraudGuard: Enterprise Command Center")

tab1, tab2, tab3 = st.tabs(["📈 Real-time Monitor", "🗄️ Database Inspector", "🤖 Airflow & MLOps"])

# ================= TAB 1: REAL-TIME MONITOR =================
with tab1:
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader("Live Anomaly Detection (Real-time)")
        # Tạo khung biểu đồ rỗng ban đầu
        chart_container = st.line_chart([], height=350)
        
    with col2:
        st.subheader("Recent Alerts")
        alert_placeholder = st.empty()
    
    # Nút điều khiển
    start_btn = st.button("🔴 KẾT NỐI LIVE STREAM", key="btn_stream")
    
    if start_btn:
        try:
            consumer = KafkaConsumer(
                'fraud_predictions',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000 # Timeout 10s
            )
            st.toast("Đã kết nối Kafka thành công! Đang chờ dữ liệu...")
            
            msg_count = 0
            
            for msg in consumer:
                msg_count += 1
                record = msg.value
                score = record.get('anomaly_score', 0)
                is_fraud = record.get('is_fraud_prediction', False)
                amount = record.get('amount', 0)
                
                # --- LOGIC VẼ BIỂU ĐỒ MƯỢT MÀ ---
                # Thay vì gửi cả list, ta chỉ gửi ĐIỂM MỚI NHẤT vào add_rows
                new_data = pd.DataFrame({'Score': [score]})
                chart_container.add_rows(new_data)
                
                # Hiển thị Alert nếu là gian lận
                if is_fraud:
                    alert_placeholder.error(
                        f"⚠️ PHÁT HIỆN GIAN LẬN!\n\n"
                        f"💰 Amount: ${amount:,.0f}\n"
                        f"📈 Score: {score:.2f}"
                    )
                
                # Reset biểu đồ nếu quá dài (tránh tràn RAM trình duyệt)
                if msg_count % 1000 == 0:
                    chart_container = st.line_chart([], height=350)

        except Exception as e:
            st.error(f"Mất kết nối Kafka hoặc không có dữ liệu: {e}")

# ================= TAB 2: DATABASE INSPECTOR =================
with tab2:
    st.header("🗄️ Dữ liệu Gian lận trong PostgreSQL")
    
    c1, c2 = st.columns([1, 4])
    with c1:
        if st.button("🔄 REFRESH DATA"):
            try:
                conn = DBManager.get_connection()
                if conn:
                    # Lấy 100 dòng mới nhất
                    query = "SELECT * FROM fraud_logs ORDER BY id DESC LIMIT 100"
                    df_logs = pd.read_sql(query, conn)
                    st.session_state['df_logs'] = df_logs
                    conn.close()
                    st.success(f"Đã tải {len(df_logs)} dòng.")
                else:
                    st.error("Không kết nối được Database.")
            except Exception as e:
                st.error(f"Lỗi DB: {e}")

    with c2:
        if 'df_logs' in st.session_state:
            df = st.session_state['df_logs']
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Thống kê nhanh
            m1, m2 = st.columns(2)
            m1.metric("Số vụ gian lận", len(df))
            m2.metric("Tổng tiền ngăn chặn", f"${df['amount'].sum():,.0f}")

# ================= TAB 3: AIRFLOW / MLOPS =================
with tab3:
    st.header("🤖 MLOps Control Plane")
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🛠️ Manual Retrain")
        if st.button("🚀 CHẠY HUẤN LUYỆN LẠI (Retrain)"):
            with st.status("Running Pipeline...", expanded=True) as status:
                st.write("Checking Data Source...")
                time.sleep(1)
                st.write("Training Autoencoder Model...")
                
                # Gọi script train
                try:
                    # Chạy từ thư mục gốc
                    res = subprocess.run(["python", "src/ml/trainer.py"], cwd=root_dir, capture_output=True, text=True)
                    if res.returncode == 0:
                        status.update(label="Training Completed!", state="complete")
                        st.success("✅ Model mới đã được lưu và sẵn sàng deploy!")
                    else:
                        status.update(label="Training Failed!", state="error")
                        st.error(f"Lỗi: {res.stderr}")
                except Exception as e:
                    st.error(f"Không thể chạy script: {e}")
    
    with c2:
        st.subheader("📅 Scheduler Status")
        st.info("Trạng thái: ACTIVE")
        st.json({
            "Pipeline": "fraud_model_retraining",
            "Schedule": "@daily",
            "Last Run": time.strftime("%Y-%m-%d 00:00:00"),
            "Next Run": "Tomorrow"
        })