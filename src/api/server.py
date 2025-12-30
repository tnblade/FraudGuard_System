# src/api/server.py
# API server để nhận giao dịch và truy vấn cảnh báo gian lận

# src/api/server.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import uvicorn
import time
import asyncio
import subprocess

# --- CẤU HÌNH ---
KAFKA_SERVER = 'localhost:9092'
MAX_RETRIES = 5

# Biến global
producer = None

# --- HÀM KHỞI TẠO (LIFESPAN) ---
# Giúp API chờ Kafka sẵn sàng rồi mới chạy
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    print("⏳ Đang kết nối tới Kafka Producer...")
    for i in range(MAX_RETRIES):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("✅ Kafka Producer đã kết nối thành công!")
            break
        except Exception as e:
            print(f"⚠️ Lần thử {i+1}/{MAX_RETRIES} thất bại: {e}")
            time.sleep(2)
    
    if producer is None:
        print("❌ CẢNH BÁO: Không thể kết nối Kafka. API sẽ chạy ở chế độ Offline (Lỗi khi gửi tin).")
    
    yield
    
    # Dọn dẹp khi tắt server
    if producer:
        producer.close()
        print("🛑 Đã đóng Kafka Producer.")

app = FastAPI(title="FraudGuard API", lifespan=lifespan)

# Thêm CORS để cho phép Web/Mobile gọi vào
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DATA MODEL ---
class TransactionRequest(BaseModel):
    step: int = 1
    type: str
    amount: float
    nameOrig: str
    oldbalanceOrg: float
    newbalanceOrig: float
    nameDest: str
    oldbalanceDest: float
    newbalanceDest: float
    isFraud: int = 0
    isFlaggedFraud: int = 0

# --- ENDPOINTS ---
@app.get("/")
def home():
    return {"status": "running", "message": "FraudGuard API is ready!"}

@app.post("/api/v1/transaction")
async def receive_transaction(tx: TransactionRequest):
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka Producer chưa sẵn sàng")

    data = tx.dict()
    try:
        # Gửi bất đồng bộ để API phản hồi nhanh hơn
        producer.send('raw_transactions', value=data)
        producer.flush() # Đẩy tin đi ngay lập tức
        return {
            "status": "received", 
            "amount": data['amount'],
            "message": "Transaction queued for AI processing"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/retrain")
async def trigger_retrain():
    """
    Endpoint để Airflow từ xa gọi vào kích hoạt việc Train Model
    """
    try:
        # Chạy file trainer.py dưới nền
        # Lưu ý: Cần đường dẫn tuyệt đối
        subprocess.Popen(["python", "/kaggle/working/FraudGuard_System/src/ml/trainer.py"])
        return {"status": "success", "message": "Pipeline huấn luyện đã được kích hoạt!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)