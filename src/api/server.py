# src/api/server.py
# API server để nhận giao dịch và truy vấn cảnh báo gian lận

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import uvicorn

app = FastAPI(title="FraudGuard API Gateway")

# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Định nghĩa dữ liệu đầu vào (Data Model)
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

@app.post("/api/v1/transaction")
async def process_transaction(transaction: TransactionRequest):
    """
    Endpoint nhận giao dịch từ bên ngoài (Web/Mobile App)
    """
    data = transaction.dict()
    
    try:
        # 1. Đẩy vào Kafka để Spark xử lý ngầm (Async Processing)
        producer.send('raw_transactions', value=data)
        
        return {
            "status": "received", 
            "message": "Giao dịch đang được xử lý bởi AI",
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Chạy server tại port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)