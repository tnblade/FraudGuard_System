# api_server.py
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
from pyngrok import ngrok, conf
import uvicorn
import json
import database
import configs

app = FastAPI()

# Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[configs.KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
except:
    producer = None
    print("⚠️ Kafka Producer not connected (Server running in limited mode)")

class Transaction(BaseModel):
    step: int
    type: str
    amount: float
    nameOrig: str
    oldbalanceOrg: float
    newbalanceOrig: float
    nameDest: str
    oldbalanceDest: float
    newbalanceDest: float
    isFraud: int

@app.post("/send_transaction")
async def send_transaction(tx: Transaction):
    data = tx.dict()
    # Logic One-hot encoding giả lập để khớp với Schema Spark
    # Trong thực tế, Client gửi raw, Spark sẽ transform. 
    # Để đơn giản demo, ta map sơ bộ ở đây hoặc Client phải gửi đúng format.
    # Ta giả định Client gửi Raw, ta cần biến đổi nhẹ trước khi bắn Kafka:
    enriched_data = data.copy()
    types = ['CASH_IN', 'CASH_OUT', 'DEBIT', 'PAYMENT', 'TRANSFER']
    for t in types:
        enriched_data[f"type_{t}"] = 1.0 if data['type'] == t else 0.0
    
    if producer:
        producer.send(configs.KAFKA_TOPIC, value=enriched_data)
        return {"status": "queued"}
    return {"status": "error", "message": "Kafka unavailable"}

@app.get("/alerts")
async def get_alerts():
    conn = database.get_connection()
    if not conn: return []
    cur = conn.cursor()
    cur.execute("SELECT timestamp, amount, anomaly_score, type FROM fraud_alerts ORDER BY id DESC LIMIT 20")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"time": str(r[0]), "amount": r[1], "score": r[2], "type": r[3]} for r in rows]

def run():
    # Setup Ngrok
    if configs.NGROK_AUTH_TOKEN:
        conf.get_default().auth_token = configs.NGROK_AUTH_TOKEN
        ngrok.kill()
        try:
            url = ngrok.connect(8000).public_url
            print(f"🌍 API PUBLIC URL: {url}/send_transaction")
            with open("api_url.txt", "w") as f: f.write(url)
        except Exception as e:
            print(f"Ngrok Error: {e}")
            
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    run()