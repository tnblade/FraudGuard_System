# dashboard.py
import streamlit as st
import pandas as pd
import time
import altair as alt
import database

st.set_page_config(page_title="Fraud Monitor", layout="wide")
st.title("🕵️ REAL-TIME FRAUD DETECTION CENTER")

col1, col2 = st.columns(2)
metrics_placeholder = st.empty()
chart_placeholder = st.empty()
alert_placeholder = st.empty()

while True:
    conn = database.get_connection()
    if conn:
        try:
            query = "SELECT * FROM fraud_alerts ORDER BY id DESC LIMIT 100"
            df = pd.read_sql(query, conn)
            conn.close()
            
            if not df.empty:
                with metrics_placeholder.container():
                    c1, c2 = st.columns(2)
                    c1.metric("Tổng Cảnh báo", len(df))
                    c2.metric("Mức độ Nguy hiểm Max", f"{df['anomaly_score'].max():.2f}")

                chart = alt.Chart(df).mark_circle(size=60).encode(
                    x='timestamp',
                    y='amount',
                    color=alt.Color('anomaly_score', scale=alt.Scale(scheme='reds')),
                    tooltip=['amount', 'anomaly_score', 'type']
                ).interactive()
                chart_placeholder.altair_chart(chart, use_container_width=True)
                
                # Alert
                last_tx = df.iloc[0]
                alert_placeholder.error(f"🚨 MỚI NHẤT: {last_tx['amount']}$ - {last_tx['type']} (Score: {last_tx['anomaly_score']:.2f})")
        except:
            pass
            
    time.sleep(2)