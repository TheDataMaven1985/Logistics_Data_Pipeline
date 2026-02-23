import json
import os
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import boto3
from io import BytesIO

# --- Configuration ---
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'Logistics-consumer-group',
    'auto.offset.reset': 'earliest'
}

# --- MinIO connection (Local S3) ---
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',  # Default MinIO Port
    aws_access_key_id='minioadmin',        # Default MinIO Credentials
    aws_secret_access_key='minioadmin',
)

BUCKET_NAME = "bronze"
TOPIC = "logistics-events"
BATCH_SIZE = 50   # How many messages to collect before saving to file

def upload_to_minio(df, bucket, object_name):
    """ Writes a DataFrane to S3(MinIO) as a Parquet file. """
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)

        s3_client.put_object(
            Bucket=bucket,
            Key=object_name,
            Body=buffer.getvalue()
        )

        print(f"Successfully updated {object_name} to MinIO with {len(df)} records.")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")

def run_consumer():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([TOPIC])

    msg_batch = []
    record_count = 0

    print(f"Consumer started...")
    print(f"Listening for messages on '{TOPIC}'...")
    print(f"Will upload batch of {BATCH_SIZE} messages to MinIO\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            try:
                # Decode and collect message
                data = json.loads(msg.value().decode('utf-8'))
                msg_batch.append(data)
                record_count += 1

                # Check if we've reached the batch size
                if len(msg_batch) >= BATCH_SIZE:
                    df = pd.DataFrame(msg_batch)

                    # Create a unique filename based on timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"logistics-events-{timestamp}.parquet"

                    print(f"\nBatch reached {BATCH_SIZE} messages. Uploading to MinIO...")
                    upload_to_minio(df, BUCKET_NAME, filename)

                    # Clear batch
                    msg_batch = []
                    print(f"Batch cleared. Waiting for next {BATCH_SIZE} messages...\n")
            except Exception as e:
                print(f"Error decoding json: {e}")
                continue

    except KeyboardInterrupt:
        print("\n\nStopping consumer...")

        # Upload remaining messages if any
        if len(msg_batch) > 0:
            print(f"Uploading remaining {len(msg_batch)} messages...")
            df = pd.DataFrame(msg_batch)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"logistics-events-final-{timestamp}.parquet"
            upload_to_minio(df, BUCKET_NAME, filename)

    finally:
        consumer.close()
        print("Consumer closed!")

if __name__ == "__main__":
    run_consumer()