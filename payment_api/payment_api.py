from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import redis
import json

app = FastAPI()


producer = KafkaProducer(bootstrap_servers='payments_kafka:9092')
redis_client = redis.Redis(host='payments_redis', password='yourpassword', port=6379)


class PaymentRequest(BaseModel):
    card_number: str
    amount: float
    currency: str
    cvv: str
    user_id: str


@app.post('/payments')
async def process_payment(payment: PaymentRequest):
    if not payment.card_number.startswith(('4', '5')):
        raise HTTPException(400, 'Неверный формат карты')
    if redis_client.incr(f'user:{payment.user_id}:count') > 10:
        raise HTTPException(429, 'Слишком частые запросы')
    message = {
        'user_id': payment.user_id,
        'amount': payment.amount,
        'card_mask': payment.card_number[-4:]
    }
    producer.send('payments', json.dumps(message).encode())

    return {'status': 'processing'}
