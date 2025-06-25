from aiokafka import AIOKafkaConsumer
import json
import logging
import asyncio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def consume_messages():
    consumer = AIOKafkaConsumer(
        'payments',
        bootstrap_servers='payments_kafka:9092',
        group_id='araud-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    await consumer.start()
    logger.info('Сервис обнаружения Фрода запущен и ожидает сообщений...')

    try:
        async for msg in consumer:
            payment = msg.value
            if payment['amount'] > 1000:
                logger.error(f'🚨 Фрод обнаружен, алярм: {payment}')
    finally:
        await consumer.stop()


async def main():
    await consume_messages()


if __name__ == '__main__':
    asyncio.run(main())
