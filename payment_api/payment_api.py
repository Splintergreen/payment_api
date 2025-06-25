from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import redis.asyncio as redis
import json
from contextlib import asynccontextmanager
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
from datetime import datetime
import logging
from uuid import uuid4

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = 'payments_kafka:9092'
REDIS_HOST = 'payments_redis'
REDIS_PASSWORD = 'yourpassword'
REDIS_PORT = 6379


# Асинхронная инициализация ресурсов
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация Redis
    app.state.redis = redis.Redis(
        host=REDIS_HOST,
        password=REDIS_PASSWORD,
        port=REDIS_PORT,
        decode_responses=True
    )
    await FastAPILimiter.init(app.state.redis)

    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await app.state.kafka_producer.start()
    logger.info('Продюсер Kafka запущен!')

    yield

    await app.state.redis.close()
    await app.state.kafka_producer.stop()
    logger.info('Подключение к Redis и Kafka закрыты.')

app.router.lifespan_context = lifespan


class PaymentRequest(BaseModel):
    user_id: str
    # payment_id: str
    amount: float
    currency: str
    card_number: str
    status: str


async def validate_card(card_number: str, redis_client: redis.Redis) -> bool:
    cache_key = f'card:validate:{card_number}'
    cached = await redis_client.get(cache_key)

    if cached:
        logger.info(f'Карта {card_number[:6]}... возвращена из кеша (значение: {cached})')
        return cached == '1'

    is_valid = card_number.startswith(('4', '5')) and len(card_number) in (15, 16)
    await redis_client.setex(cache_key, 3600, '1' if is_valid else '0')
    return is_valid


@app.post('/payments', dependencies=[Depends(RateLimiter(times=10, seconds=60))])
async def process_payment(payment: PaymentRequest):
    if not await validate_card(payment.card_number, app.state.redis):
        raise HTTPException(status_code=400, detail='Невалидный номер карты!')
    payment_id = str(uuid4())
    message = {
        'user_id': payment.user_id,
        'payment_id': payment_id,
        'amount': payment.amount,
        'currency': payment.currency,
        'card_mask': payment.card_number[-4:],
        'status': 'processing',
        'send_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    try:
        await app.state.kafka_producer.send_and_wait(
            'payments',
            value=message
        )
        logger.info(f'Платеж отправлен в Kafka: {message}')
    except Exception as e:
        logger.error(f'Ошибка при отправке платежа в Kafka: {e}')
        raise HTTPException(status_code=500, detail='Ошибка при обработке платежа!')

    await app.state.redis.setex(f"payment:{payment_id}", 86400, json.dumps(message))  #24 часа
    return {'payment_id': payment_id, 'status': 'processing', 'message': 'Платеж обрабатывается!'}


@app.get('/payments/{payment_id}')
async def get_payment_status(payment_id: str):
    payment_data = await app.state.redis.get(f'payment:{payment_id}')
    if not payment_data:
        raise HTTPException(status_code=404, detail='Payment not found')
    logger.info(f'Платеж {payment_id} возвращен из кеша')
    return json.loads(payment_data)
