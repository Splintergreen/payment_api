import aio_pika
import json
from datetime import datetime
import logging
from typing import Optional


rabbit_conn = 'amqp://admin:admin@payments_rabbitmq/'


class LoggingService:
    def __init__(self, rabbitmq_url: str, queue_name: str = 'logs'):
        self.rabbitmq_url = rabbitmq_url
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.queue = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True
        )

    async def close(self):
        if self.connection:
            await self.connection.close()

    async def log(self, level: str, message: str, request_id: Optional[str] = None, **extra):
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': level,
            'message': message,
            'service': 'payments',
            **extra
        }

        if request_id:
            log_data['request_id'] = request_id

        try:
            await self.queue.channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(log_data).encode(),
                    headers={'request_id': request_id} if request_id else {},
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=self.queue_name
            )
        except Exception as e:
            logging.error(f'Logging error: {str(e)}')


logging_service = LoggingService(rabbit_conn)
