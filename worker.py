# worker.py
import json
import os
import pika
import time


EXCHANGE = os.getenv("DRIVENOW_MQ_EXCHANGE", "drivenow.events")
QUEUE = os.getenv("DRIVENOW_MQ_QUEUE", "drivenow.audit")
BINDING_KEY = os.getenv("DRIVENOW_MQ_BINDING_KEY", "drivenow.#")
URL = os.getenv("DRIVENOW_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

def on_message(ch, method, properties, body: bytes):
    try:
        msg = json.loads(body.decode("utf-8"))
        print(f"[WORKER] received event: type={msg.get('type')} payload={msg.get('payload')}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[WORKER] error handling message: {e}")
        # reject + don't requeue to avoid infinite loops
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    while True:
        try:
            params = pika.URLParameters(URL)
            conn = pika.BlockingConnection(params)
            ch = conn.channel()

            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

            ch.queue_declare(queue=QUEUE, durable=True)
            ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=BINDING_KEY)

            ch.basic_qos(prefetch_count=10)
            ch.basic_consume(queue=QUEUE, on_message_callback=on_message)

            print("[WORKER] waiting for events...")
            ch.start_consuming()
        except Exception as e:
            print(f"[WORKER] connection error, retrying in 2s: {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()