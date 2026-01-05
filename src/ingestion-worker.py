import pika
import json
import io
import uuid
from minio import Minio
from datetime import datetime

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False 
)

BUCKET_NAME = "raw-data-lake"

if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' berhasil dibuat.")

def callback(ch, method, properties, body):
    try:
        print(f"DEBUG - Data yang diterima: {body}")

        log_data = json.loads(body)

        message = log_data.get("msg", "NO_MESSAGE")

        print(f" [v] Log diterima | app={log_data.get('app_name')} | severity={log_data.get('severity')} | msg={message}")

        now = datetime.now()
        filename = f"logs/{now.year}/{now.month:02d}/{now.day:02d}/{now.strftime('%H%M%S')}-{uuid.uuid4()}.json"

        data_bytes = json.dumps(log_data).encode("utf-8")
        data_stream = io.BytesIO(data_bytes)

        minio_client.put_object(
            BUCKET_NAME,
            filename,
            data_stream,
            length=len(data_bytes),
            content_type="application/json"
        )

        print(f" [^] Uploaded ke MinIO: {filename}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")

def start_ingestion():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
    )
    channel = connection.channel()

    channel.queue_declare(queue='log_queue', durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='log_queue', on_message_callback=callback)

    print(' [*] Menunggu pesan log... Tekan CTRL+C untuk berhenti')
    channel.start_consuming()

if __name__ == "__main__":
    start_ingestion()