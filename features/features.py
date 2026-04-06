import json
import time
import pika
import numpy as np
from datetime import datetime
from sklearn.datasets import load_diabetes


# Load dataset once at startup
data = load_diabetes()
X = data.data
y = data.target


def get_connection():
    """Connect to RabbitMQ with retry logic."""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rabbitmq', heartbeat=600)
            )
            return connection
        except Exception as e:
            print(f"[features] Waiting for RabbitMQ: {e}")
            time.sleep(5)


connection = get_connection()
channel = connection.channel()

channel.queue_declare(queue='X', durable=True)
channel.queue_declare(queue='y_true', durable=True)

print("[features] Starting infinite data stream...")

while True:
    # Pick a random observation
    random_row = np.random.randint(0, len(X))
    message_id = datetime.timestamp(datetime.now())

    message_X = {
        "id": message_id,
        "body": X[random_row].tolist()
    }
    message_y_true = {
        "id": message_id,
        "body": float(y[random_row])
    }

    channel.basic_publish(
        exchange='',
        routing_key='X',
        body=json.dumps(message_X),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    channel.basic_publish(
        exchange='',
        routing_key='y_true',
        body=json.dumps(message_y_true),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    print(f"[features] Sent id={message_id}, y_true={message_y_true['body']}")
    time.sleep(10)
