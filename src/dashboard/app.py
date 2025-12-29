# src/dashboard/app.py
# Dashboard Streamlit để hiển thị cảnh báo gian lận

import streamlit as st
import pandas as pd
import time
from src.database.db_manager import DBManager

st.title("🛡️ Fraud Guard Dashboard")

placeholder = st.empty()

while True:
    conn = DBManager.get_connection()
    if conn:
        df = pd.read_sql("SELECT * FROM fraud_alerts ORDER BY id DESC LIMIT 50", conn)
        with placeholder.container():
            st.dataframe(df)
            if not df.empty:
                st.error(f"Last Alert: {df.iloc[0]['amount']}")
    time.sleep(2)