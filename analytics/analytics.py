import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import insert, MetaData, Table, Column, Integer, String, DECIMAL, DateTime, Boolean
from sqlalchemy.dialects.postgresql import UUID
from aiokafka import AIOKafkaConsumer
import asyncio
import logging
from typing import Dict, Any
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaymentErrorExeption(Exception):
    pass


class ErrorConsumeMessage(Exception):
    pass


class PaymentAnalytics:
    def __init__(self):
        self.kafka_servers = 'payments_kafka:9092'
        self.db_url = 'postgresql+asyncpg://postgres:postgres@payments_postgres:5432/payments'
        self.engine = None
        self.metadata = MetaData()
        self.transactions = None
        self.consumer = None
        self.running = False

    async def init_db(self) -> None:
        max_retries = 5
        retry_delay = 5

        for attempt in range(1, max_retries + 1):
            try:
                self.transactions = Table(
                    'transactions', self.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('user_id', String(20), nullable=False),
                    Column('payment_id', UUID(as_uuid=True)),
                    Column('amount', DECIMAL(50, 2), nullable=False),
                    Column('card_mask', String(4), nullable=False),
                    Column('status', String(20), nullable=False),
                    Column('send_at', DateTime),
                    Column('created_at', DateTime),
                    Column('fraud', Boolean, default=False)
                )

                self.engine = create_async_engine(
                    self.db_url,
                    pool_size=10,
                    max_overflow=5,
                    echo=False
                )

                async with self.engine.begin() as conn:
                    await conn.run_sync(self.metadata.create_all)
                logger.info('Таблица transactions создана!')
                return
            except Exception as e:
                logger.error(f'Попытка №{attempt}/{max_retries} создать таблицу неуспешна: {str(e)}')
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
        raise Exception('Не удалось создать таблицу!!!')

    async def create_kafka_consumer(self) -> AIOKafkaConsumer:
        max_retries = 5
        retry_delay = 5

        for attempt in range(1, max_retries + 1):
            try:
                consumer = AIOKafkaConsumer(
                    'payments',
                    bootstrap_servers=self.kafka_servers,
                    group_id='analytic-group',
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=False
                )
                await consumer.start()
                logger.info('Консьюмер analytic-group успешно создан!')
                return consumer
            except Exception as e:
                logger.error(f'Попытка подключения №{attempt}/{max_retries} к Кафке неуспешна: {str(e)}')
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
        raise ConnectionError('Не удалось подключиться к Кафке!!!')

    async def process_payment(self, payment: Dict[str, Any]) -> None:
        try:
            logger.info(f'Обработка платежа: {payment}')

            async with AsyncSession(self.engine) as session:
                query = insert(self.transactions).values(
                    user_id=payment['user_id'],
                    payment_id=payment['payment_id'],
                    amount=payment['amount'],
                    card_mask=payment['card_mask'],
                    status='completed',
                    send_at=datetime.strptime(payment['send_at'], '%Y-%m-%d %H:%M:%S'),
                    created_at=datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
                )
                await session.execute(query)
                await session.commit()

            logger.info('Платеж успешно записан в БД!')
        except Exception as e:
            logger.error(f'Ошибка при проведении платежа: {str(e)}')
            raise PaymentErrorExeption('Ошибка при проведении платежа!')

    async def consume_messages(self) -> None:
        try:
            async for message in self.consumer:
                try:
                    logger.debug(f'Чтение сообщения, топик: {message.topic}, партиция: {message.partition}, офсет: {message.offset}')
                    await self.process_payment(message.value)
                    await self.consumer.commit()
                except Exception as e:
                    logger.error(f'Ошибка при чтении сообщения, сообщение будет пропущено: {str(e)}')
                    continue
        except Exception as e:
            logger.critical(f'Ошибка обработки сообщений из Кафки: {str(e)}', exc_info=True)
            raise ErrorConsumeMessage('Ошибка обработки сообщений из Кафки!')

    async def run(self) -> None:
        self.running = True
        try:
            await self.init_db()
            self.consumer = await self.create_kafka_consumer()
            logger.info('Сервис Аналитики запущен!')
            await self.consume_messages()
        except asyncio.CancelledError:
            logger.info('Сервис Аналитики останавливается!')
        except Exception as e:
            logger.critical(f'Ошибка запуска сервиса: {str(e)}', exc_info=True)
        finally:
            self.running = False
            await self.cleanup()

    async def cleanup(self) -> None:
        if self.consumer:
            try:
                await self.consumer.stop()
                logger.info('Консьюмер analytic-group остановлен!')
            except Exception as e:
                logger.error(f'Ошибка при остановке консьюмера analytic-group: {str(e)}')

        if self.engine:
            try:
                await self.engine.dispose()
                logger.info('Подключение к БД остановлено!')
            except Exception as e:
                logger.error(f'Ошибка при отключении от БД: {str(e)}')


async def main():
    analytics = PaymentAnalytics()
    await analytics.run()


if __name__ == '__main__':
    asyncio.run(main())
