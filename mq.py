# mq.py
import json
import os
import pika


DEFAULT_EXCHANGE = os.getenv("DRIVENOW_MQ_EXCHANGE", "drivenow.events")
DEFAULT_ROUTING_KEY = os.getenv("DRIVENOW_MQ_ROUTING_KEY", "drivenow.event")
DEFAULT_URL = os.getenv("DRIVENOW_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
routing_key = "drivenow.car.added"

class EventPublisher:
    """
    Small RabbitMQ publisher.
    - Declares a durable topic exchange
    - Publishes JSON messages
    """

    def __init__(self, url: str = DEFAULT_URL, exchange: str = DEFAULT_EXCHANGE):
        self.url = url
        self.exchange = exchange

    def publish(self, event_type: str, payload: dict, routing_key: str = DEFAULT_ROUTING_KEY) -> None:
        message = {
            "type": event_type,
            "payload": payload,
        }

        params = pika.URLParameters(self.url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)

        print(f"DEBUG: publishing to exchange={self.exchange}, routing_key={routing_key}, message={message}")
        
        channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=json.dumps(message).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistent
            ),
        )

        connection.close()