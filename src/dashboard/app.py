# src/dashboard/app.py
# Dashboard Streamlit để hiển thị cảnh báo gian lận

import streamlit as st
import pandas as pd
import json
import time
from kafka import KafkaConsumer

# --- CẤU HÌNH ---
st.set_page_config(
    page_title="FraudGuard Monitor",
    page_icon="🛡️",
    layout="wide"
)

# --- CSS TÙY CHỈNH (Giao diện Dark Mode đẹp hơn) ---
st.markdown("""
<style>
    .metric-card {
        background-color: #262730;
        padding: 15px;
        border-radius: 10px;
        color: white;
    }
    .stAlert {
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# --- TIÊU ĐỀ ---
st.title("🛡️ FraudGuard System - Realtime Monitoring")
st.markdown("Hệ thống giám sát giao dịch và phát hiện gian lận qua Kafka Streaming")

# --- KHỞI TẠO LAYOUT ---
# Cột 1: Biểu đồ Live, Cột 2: Danh sách Cảnh báo
col_chart, col_alerts = st.columns([2, 1])

with col_chart:
    st.subheader("📉 Anomaly Score (Độ bất thường)")
    chart_placeholder = st.empty()

with col_alerts:
    st.subheader("🚨 Cảnh báo Gian lận (Live)")
    alert_placeholder = st.empty()

# --- METRIC TỔNG QUAN ---
metric_placeholder = st.empty()

# --- HÀM KẾT NỐI KAFKA ---
@st.cache_resource
def get_consumer():
    # Dùng cache_resource để không tạo lại connection mỗi lần refresh
    return KafkaConsumer(
        'fraud_predictions',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

# --- LOGIC CHÍNH ---
def run_dashboard():
    consumer = get_consumer()
    
    # Bộ nhớ đệm dữ liệu để hiển thị
    data_window = []  # Lưu score để vẽ biểu đồ
    alert_window = [] # Lưu log cảnh báo
    total_processed = 0
    fraud_count = 0

    print("🚀 Dashboard đã sẵn sàng nhận dữ liệu từ Kafka...")

    for message in consumer:
        record = message.value
        
        # 1. Lấy thông tin từ gói tin JSON
        # (Lưu ý: tên trường phải khớp với output của Spark Job)
        amount = record.get('amount', 0)
        score = record.get('anomaly_score', 0)
        is_fraud = record.get('is_fraud_prediction', False)
        
        # Cập nhật đếm
        total_processed += 1
        
        # 2. Xử lý Logic Hiển thị
        # Thêm vào dữ liệu biểu đồ
        current_time = time.strftime("%H:%M:%S")
        data_window.append({"Time": current_time, "Score": score})
        
        # Giới hạn cửa sổ biểu đồ (chỉ hiện 100 điểm gần nhất cho mượt)
        if len(data_window) > 100: 
            data_window.pop(0)

        # Xử lý Cảnh báo (Nếu là Fraud)
        if is_fraud:
            fraud_count += 1
            alert_msg = {
                "Thời gian": current_time,
                "Số tiền": f"${amount:,.2f}",
                "Độ lệch": f"{score:.4f}"
            }
            alert_window.insert(0, alert_msg) # Thêm vào đầu danh sách
            if len(alert_window) > 10: 
                alert_window.pop() # Chỉ giữ 10 cảnh báo mới nhất

        # 3. Render lên giao diện (Cập nhật sau mỗi 5 gói tin để giảm lag UI)
        if total_processed % 5 == 0:
            
            # Cập nhật Metrics
            with metric_placeholder.container():
                c1, c2, c3 = st.columns(3)
                c1.metric("Tổng giao dịch", total_processed)
                c2.metric("Gian lận phát hiện", fraud_count, delta_color="inverse")
                c3.metric("Ngưỡng (Threshold)", "0.05")

            # Vẽ biểu đồ
            with chart_placeholder:
                df_chart = pd.DataFrame(data_window)
                if not df_chart.empty:
                    st.line_chart(df_chart.set_index("Time")['Score'], color="#ff4b4b")

            # Hiển thị bảng cảnh báo
            with alert_placeholder:
                if alert_window:
                    st.error(f"⚠️ Đã phát hiện {len(alert_window)} cảnh báo mới!")
                    st.table(pd.DataFrame(alert_window))
                else:
                    st.success("✅ Hệ thống bình thường")

if __name__ == "__main__":
    try:
        run_dashboard()
    except Exception as e:
        st.error(f"Lỗi kết nối: {e}")
        st.info("Hãy chắc chắn rằng Kafka và Spark đang chạy!")