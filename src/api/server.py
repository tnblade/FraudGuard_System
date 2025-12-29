# src/api/server.py
# API server để nhận giao dịch và truy vấn cảnh báo gian lận

from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json
from src.core.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.database.db_manager import DBManager

app = FastAPI()

# Producer Init
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
except: producer = None

class Transaction(BaseModel):
    amount: float
    type: str
    # ... fields khác ...

@app.post("/send")
def send_tx(tx: Transaction):
    if producer:
        producer.send(KAFKA_TOPIC, tx.dict())
        return {"status": "sent"}
    return {"status": "error"}

@app.get("/alerts")
def get_alerts():
    conn = DBManager.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM fraud_alerts ORDER BY id DESC LIMIT 10")
    rows = cur.fetchall()
    return rows