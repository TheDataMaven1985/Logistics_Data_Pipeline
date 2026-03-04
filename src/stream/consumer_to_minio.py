import json
import os
import time
import sys
import argparse
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import boto3
from io import BytesIO

# --- Configuration ---
KAFKA_BOOTSTRAP  = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

KAFKA_CONF = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'Logistics-consumer-group',
    'auto.offset.reset': 'earliest'
}

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

BUCKET_NAME = "bronze"
TOPIC       = "logistics-events"
BATCH_SIZE  = 50


def upload_to_minio(df, bucket, object_name):
    """Writes a DataFrame to S3 (MinIO) as a Parquet file."""
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=object_name, Body=buffer.getvalue())
        print(f"Successfully uploaded {object_name} to MinIO with {len(df)} records.")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")


def run_consumer(duration: int = None):
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([TOPIC])

    msg_batch = []
    start_time = time.time()

    print(f"Consumer started...")
    print(f"Kafka broker  : {KAFKA_BOOTSTRAP}")
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"Listening on  : '{TOPIC}'")
    print(f"Batch size    : {BATCH_SIZE}")
    print(f"Duration      : {duration}s\n" if duration else "Duration      : unlimited\n")

    try:
        while True:

            # Stop after duration seconds if specified
            if duration and (time.time() - start_time) >= duration:
                print(f"\nDuration of {duration}s reached. Stopping consumer...")
                break

            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:  # ← fixed
                    continue
                else:
                    print(msg.error())
                    sys.exit(1)
                    break

            try:
                data = json.loads(msg.value().decode('utf-8'))
                msg_batch.append(data)

                if len(msg_batch) >= BATCH_SIZE:
                    df = pd.DataFrame(msg_batch)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"logistics-events-{timestamp}.parquet"
                    print(f"\nBatch reached {BATCH_SIZE} messages. Uploading to MinIO...")
                    upload_to_minio(df, BUCKET_NAME, filename)
                    msg_batch = []
                    print(f"Batch cleared. Waiting for next {BATCH_SIZE} messages...\n")

            except Exception as e:
                print(f"Error decoding json: {e}")
                continue

    except KeyboardInterrupt:
        print("\n\nStopping consumer...")

    finally:
        # Upload any remaining messages before closing
        if msg_batch:
            print(f"Uploading remaining {len(msg_batch)} messages...")
            df = pd.DataFrame(msg_batch)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"logistics-events-final-{timestamp}.parquet"
            upload_to_minio(df, BUCKET_NAME, filename)

        consumer.close()
        print("Consumer closed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka to MinIO consumer")
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="How long to run the consumer in seconds (default: run forever)"
    )
    args = parser.parse_args()
    run_consumer(duration=args.duration)