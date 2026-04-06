import json
import time
import pika
import numpy as np
from sklearn.datasets import load_diabetes
from sklearn.linear_model import LinearRegression


# Train model once at startup using the full diabetes dataset
data = load_diabetes()
X_train = data.data
y_train = data.target

model = LinearRegression()
model.fit(X_train, y_train)
print("[model] Linear regression model trained.")


def get_connection():
    """Connect to RabbitMQ with retry logic."""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rabbitmq', heartbeat=600)
            )
            return connection
        except Exception as e:
            print(f"[model] Waiting for RabbitMQ: {e}")
            time.sleep(5)


connection = get_connection()
channel = connection.channel()

channel.queue_declare(queue='X', durable=True)
channel.queue_declare(queue='y_pred', durable=True)


def callback(ch, method, properties, body):
    message = json.loads(body)
    message_id = message["id"]
    features = np.array(message["body"]).reshape(1, -1)

    y_pred = model.predict(features)[0]

    message_y_pred = {
        "id": message_id,
        "body": round(float(y_pred), 2)
    }

    ch.basic_publish(
        exchange='',
        routing_key='y_pred',
        body=json.dumps(message_y_pred),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    print(f"[model] id={message_id}, y_pred={message_y_pred['body']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='X', on_message_callback=callback)

print("[model] Waiting for feature messages...")
channel.start_consuming()
