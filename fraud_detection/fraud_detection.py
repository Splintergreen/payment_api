from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'payments',
    bootstrap_servers='payments_kafka:9092',
    group_id='fraud-group'
)


logger.info('Сервис обнаружения Фрода запущен')
for msg in consumer:
    payment = json.loads(msg.value)
    if payment['amount'] > 1000:
        logger.error(f'🚨 Фрод обнаружен, алярм: {payment}')
