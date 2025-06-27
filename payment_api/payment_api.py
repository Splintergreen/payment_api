from fastapi import FastAPI, HTTPException, Depends, Request
from aiokafka import AIOKafkaProducer
import redis.asyncio as redis
import json
from contextlib import asynccontextmanager
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
from datetime import datetime
import logging
from uuid import uuid4
from schemes import PaymentRequest
from rabbit_logging import logging_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = 'payments_kafka:9092'
REDIS_HOST = 'payments_redis'
REDIS_PASSWORD = 'yourpassword'
REDIS_PORT = 6379


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.Redis(
        host=REDIS_HOST,
        password=REDIS_PASSWORD,
        port=REDIS_PORT,
        decode_responses=True
    )
    await FastAPILimiter.init(app.state.redis)
    logger.info('Подключен к Redis!')
    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await app.state.kafka_producer.start()
    logger.info('Продюсер Kafka запущен!')
    await logging_service.connect()
    logger.info('Логирование и отправка в Rabbit запущено!')

    yield

    await app.state.kafka_producer.stop()
    await logging_service.close()
    await app.state.redis.close()
    logger.info('Kafka, Redis, Logger остановлены успешно!')

app.router.lifespan_context = lifespan


@app.middleware('http')
async def add_request_id(request: Request, call_next):
    request_id = str(uuid4())
    request.state.request_id = request_id

    try:
        response = await call_next(request)
        return response
    except Exception as e:
        await logging_service.log('ERROR', f'Request failed: {str(e)}', request_id=request_id)
        raise


async def log_event(level: str, message: str, request: Request, **extra):
    request_id = getattr(request.state, 'request_id', None)
    await logging_service.log(
        level,
        message,
        request_id=request_id,
        service='payment_api',
        **extra
    )


async def validate_card(card_number: str, redis_client: redis.Redis, request: Request) -> bool:
    cache_key = f'card:validate:{card_number}'
    cached = await redis_client.get(cache_key)

    if cached:
        await log_event('INFO', f'Карта {card_number[:6]} возвращена из кеша!', request, cached_result=cached)
        return cached == '1'

    is_valid = card_number.startswith(('4', '5')) and len(card_number) in (15, 16)

    await log_event('DEBUG', f'Проверка карты {card_number[:6]}...', request, is_valid=is_valid)
    await redis_client.setex(cache_key, 3600, '1' if is_valid else '0')
    return is_valid


@app.post('/payments', dependencies=[Depends(RateLimiter(times=10, seconds=60))])
async def process_payment(payment: PaymentRequest, request: Request):
    is_valid = await validate_card(payment.card_number, app.state.redis, request)

    if not is_valid:
        await log_event('WARNING', 'Невалидный номер карты', request, card_prefix=payment.card_number[:6])
        raise HTTPException(status_code=400, detail='Невалидный номер карты!')

    payment_id = str(uuid4())
    payment_message = {
        'payment_id': payment_id,
        'user_id': payment.user_id,
        'amount': payment.amount,
        'currency': payment.currency,
        'card_mask': payment.card_number[-4:],
        'status': 'processing',
        'send_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'request_id': request.state.request_id
    }

    try:
        await app.state.kafka_producer.send_and_wait('payments', value=payment_message)
        await log_event('INFO', 'Платеж отправлен в Kafka', request, payment_id=payment_id)
        await app.state.redis.setex(f'payment:{payment_id}', 86400, json.dumps(payment_message))

    except Exception as e:
        await log_event('ERROR', 'Ошибка обработки платежа', request, error=str(e), payment_id=payment_id)
        raise HTTPException(status_code=500, detail='Ошибка обработки платежа')

    return {
        'payment_id': payment_id,
        'status': 'processing',
        'request_id': request.state.request_id
    }


@app.get('/payments/{payment_id}')
async def get_payment_status(payment_id: str, request: Request):
    payment_data = await app.state.redis.get(f'payment:{payment_id}')
    if not payment_data:
        await log_event('WARNING', 'Платеж не найден', request, payment_id=payment_id)
        raise HTTPException(status_code=404, detail='Платеж не найден')

    await log_event('INFO', 'Запрос статуса платежа', request, payment_id=payment_id)

    return json.loads(payment_data)
