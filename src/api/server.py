# src/api/server.py
# API server để nhận giao dịch và truy vấn cảnh báo gian lận

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import uvicorn

app = FastAPI(title="FraudGuard API")

# Cấu hình Kafka Producer (Có try-catch để không sập nếu Kafka chưa lên)
def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except Exception as e:
        print(f"Warning: Kafka chưa sẵn sàng ({e})")
        return None

producer = get_producer()

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

@app.get("/")
def home():
    return {"status": "running", "message": "FraudGuard API is ready!"}

@app.post("/api/v1/transaction") # Lưu ý đường dẫn này
async def receive_transaction(tx: TransactionRequest):
    global producer
    if not producer:
        producer = get_producer()
        if not producer:
            raise HTTPException(status_code=500, detail="Kafka Connection Failed")

    data = tx.dict()
    try:
        producer.send('raw_transactions', value=data)
        producer.flush()
        return {"status": "received", "amount": data['amount']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)