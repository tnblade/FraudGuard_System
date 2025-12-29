# main.py
# Điểm khởi động chính cho toàn bộ hệ thống Fraud Detection

import subprocess
import time
from src.database.db_manager import DBManager

def main():
    # 1. Setup (Run shell script first manually)
    
    # 2. Init DB
    DBManager.init_db()
    
    # 3. Start Processes
    print("🚀 Starting Services...")
    subprocess.Popen(["python", "-m", "src.streaming.spark_job"])
    subprocess.Popen(["uvicorn", "src.api.server:app", "--host", "0.0.0.0", "--port", "8000"])
    subprocess.Popen(["streamlit", "run", "src/dashboard/app.py"])
    
    # Keep alive
    while True: time.sleep(10)

if __name__ == "__main__":
    main()