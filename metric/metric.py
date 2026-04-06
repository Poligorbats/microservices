import json
import time
import csv
import os
import pika
import threading


LOG_FILE = '/logs/metric_log.csv'

# In-memory buffer: {id: {"y_true": float|None, "y_pred": float|None}}
buffer = {}
buffer_lock = threading.Lock()


def write_log(message_id, y_true, y_pred):
    absolute_error = round(abs(y_true - y_pred), 2)
    row = {
        "id": message_id,
        "y_true": y_true,
        "y_pred": y_pred,
        "absolute_error": absolute_error
    }
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "y_true", "y_pred", "absolute_error"])
        writer.writerow(row)
    print(f"[metric] Logged id={message_id}, y_true={y_true}, y_pred={y_pred}, error={absolute_error}")


def try_flush(message_id):
    """If both y_true and y_pred are available for an id, write log and remove from buffer."""
    with buffer_lock:
        entry = buffer.get(message_id)
        if entry and entry["y_true"] is not None and entry["y_pred"] is not None:
            write_log(message_id, entry["y_true"], entry["y_pred"])
            del buffer[message_id]


def callback_y_true(ch, method, properties, body):
    message = json.loads(body)
    message_id = message["id"]
    y_true = float(message["body"])
    with buffer_lock:
        if message_id not in buffer:
            buffer[message_id] = {"y_true": None, "y_pred": None}
        buffer[message_id]["y_true"] = y_true
    ch.basic_ack(delivery_tag=method.delivery_tag)
    try_flush(message_id)


def callback_y_pred(ch, method, properties, body):
    message = json.loads(body)
    message_id = message["id"]
    y_pred = float(message["body"])
    with buffer_lock:
        if message_id not in buffer:
            buffer[message_id] = {"y_true": None, "y_pred": None}
        buffer[message_id]["y_pred"] = y_pred
    ch.basic_ack(delivery_tag=method.delivery_tag)
    try_flush(message_id)


def get_connection():
    """Connect to RabbitMQ with retry logic."""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rabbitmq', heartbeat=600)
            )
            return connection
        except Exception as e:
            print(f"[metric] Waiting for RabbitMQ: {e}")
            time.sleep(5)


# Ensure log file exists with header
os.makedirs('/logs', exist_ok=True)
if not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0:
    with open(LOG_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "y_true", "y_pred", "absolute_error"])
        writer.writeheader()

connection = get_connection()
channel = connection.channel()

channel.queue_declare(queue='y_true', durable=True)
channel.queue_declare(queue='y_pred', durable=True)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='y_true', on_message_callback=callback_y_true)
channel.basic_consume(queue='y_pred', on_message_callback=callback_y_pred)

print("[metric] Waiting for messages...")
channel.start_consuming()
