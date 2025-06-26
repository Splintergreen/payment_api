from aiokafka import AIOKafkaConsumer
import json
import logging
import asyncio
from sqlalchemy import update, Table, Column, Boolean, UUID, MetaData, String
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

DB_URL = 'postgresql+asyncpg://postgres:postgres@payments_postgres:5432/payments'

engine = create_async_engine(DB_URL, echo=True, poolclass=NullPool, future=True)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

metadata = MetaData()
transactions = Table(
    'transactions', metadata,
    Column('payment_id', UUID(as_uuid=True)),
    Column('status', String(20), nullable=False),
    Column('fraud', Boolean)
)


async def update_fraud_status(payment_id: str):
    async with AsyncSessionLocal() as session:
        try:
            stmt = (
                update(transactions)
                .where(transactions.c.payment_id == payment_id)
                .values(fraud=True)
                .values(status='declined')
            )
            await session.execute(stmt)
            await session.commit()
            logger.info(f'Обнаружен Фрод для payment_id={payment_id}')
        except Exception as e:
            await session.rollback()
            logger.error(f'Ошибка при обновлении статуса fraud: {e}')
            raise


async def process_payment(payment: dict):
    try:
        payment_id = payment.get('payment_id')
        amount = payment.get('amount', 0)

        is_fraud = False
        fraud_reason = None

        if amount > 1000:
            is_fraud = True
            fraud_reason = 'Сумма превышает 1000!!!!!'
        elif payment.get('card_mask') == '9999':
            is_fraud = True
            fraud_reason = 'Маска карты вызывает подозрение!!!!!'

        if is_fraud:
            logger.warning(f'Выявлен фрод: {fraud_reason}. Платеж: {payment}')
            await update_fraud_status(payment_id)
        else:
            logger.info(f'Платеж {payment_id} проверен, фрод не обнаружен.')

    except Exception as e:
        logger.error(f'Ошибка при обработке платежа: {e}')


async def consume_messages():
    consumer = AIOKafkaConsumer(
        'payments',
        bootstrap_servers='payments_kafka:9092',
        group_id='fraud-detection-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    await consumer.start()
    logger.info('Сервис обнаружения фрода запущен и ожидает сообщений...')

    try:
        async for msg in consumer:
            try:
                payment = msg.value
                logger.info(f'Получен платеж для проверки: {payment}')
                await process_payment(payment)
            except json.JSONDecodeError:
                logger.error('Ошибка декодирования JSON сообщения')
            except Exception as e:
                logger.error(f'Ошибка обработки сообщения: {e}')
    finally:
        await consumer.stop()


async def main():
    await consume_messages()


if __name__ == '__main__':
    asyncio.run(main())
