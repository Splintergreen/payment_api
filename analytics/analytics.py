import json
import psycopg2
from kafka import KafkaConsumer
from time import sleep
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaymentAnalytics:
    def __init__(self):
        self.kafka_servers = 'payments_kafka:9092'
        self.db_config = {
            'host': 'payments_postgres',
            'database': 'payments',
            'user': 'postgres',
            'password': 'postgres',
            'port': '5432'
        }
        self.consumer = None
        self.conn = None
        self.init_db()

    def init_db(self):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**self.db_config)
                cursor = conn.cursor()
                with open('/app/db_init/init.sql', 'r') as f:
                    cursor.execute(f.read())
                conn.commit()
                logger.info('Таблица создана!')
                return True
            except Exception as e:
                logger.error(f'Попытка № {attempt+1} создать таблицу неуспешна: {str(e)}')
                if attempt < max_retries - 1:
                    sleep(5)
        raise Exception('Не получилось создать таблицу!!!')

    def create_db_connection(self):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**self.db_config)
                logger.info('Успешное подключение к БД!')
                return conn
            except psycopg2.OperationalError as e:
                logger.warning(f'Попытка подключения № {attempt + 1} к БД неуспешна: {str(e)}')
                if attempt < max_retries - 1:
                    sleep(5)
                    continue
                raise

    def create_kafka_consumer(self):
        for attempt in range(5):
            try:
                consumer = KafkaConsumer(
                    'payments',
                    bootstrap_servers='payments_kafka:9092',
                    group_id='analytic-group'
                )
                consumer.topics()
                logger.info('Консьюмер создан успешно')
                return consumer
            except Exception as e:
                logger.error(f'Попытка подключения № {attempt+1}/5 к Кафке неуспешна: {str(e)}')
                sleep(5)
        raise ConnectionError('Не удалось подключиться к Кафке')

    def process_messages(self):
        while True:
            try:
                self.conn = self.create_db_connection()
                self.consumer = self.create_kafka_consumer()
                logger.info('Консьюмер готов, ожидает сообщений!')
                for message in self.consumer:
                    try:
                        logger.info(f'Сообщение: {message}')
                        payment = json.loads(message.value)
                        logger.info(f'Обработка платежа: {payment}')
                        with self.conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO transactions
                                (user_id, amount, card_mask, status) 
                                VALUES (%s, %s, %s, 'completed')
                            """, (payment['user_id'], payment['amount'], payment['card_mask']))
                            self.conn.commit()
                        self.consumer.commit()

                    except Exception as e:
                        logger.error(f'Ошибка при обработке сообщения: {str(e)}')
                        if self.conn:
                            self.conn.rollback()

            except Exception as e:
                logger.critical(f'Критическая ошибка: {str(e)}')
            finally:
                if hasattr(self, 'consumer') and self.consumer:
                    self.consumer.close()
                if hasattr(self, 'conn') and self.conn:
                    self.conn.close()
                logger.info('Консьюмер остановлен')
                sleep(5)


if __name__ == '__main__':
    logger.info('Сервис аналитики запущен')
    analytics = PaymentAnalytics()
    analytics.process_messages()
