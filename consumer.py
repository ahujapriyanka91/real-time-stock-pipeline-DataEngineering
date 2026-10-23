"""
Step 3: Consumer - reads prices from Kafka and saves them as files in MinIO.
Saves a file every 50 prices or every 60 seconds, then tells Kafka "done".
Run:  python consumer.py   (if stopped, unsaved prices are read again next time - nothing lost)
"""

import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")
BUCKET = "bronze"

BATCH_SIZE = 50          # save a file after 50 prices...
BATCH_SECONDS = 60       # ...or after 60 seconds, whichever comes first

# Connect to MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASSWORD,
    region_name="us-east-1",
)

# Create the bronze bucket the first time
try:
    s3.head_bucket(Bucket=BUCKET)
except ClientError:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Created bucket '{BUCKET}'")

# Connect to Kafka
consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVER,
    "group.id": "minio-writer",        # Kafka remembers how far this name has read
    "auto.offset.reset": "earliest",   # first start: read old messages too
    "enable.auto.commit": False,       # we say "done" ourselves, only after saving
})
consumer.subscribe([TOPIC])


def save_batch(batch):
    now = datetime.now(timezone.utc)
    key = f"stock_quotes/date={now:%Y-%m-%d}/quotes_{now:%H%M%S_%f}.json"
    body = "\n".join(json.dumps(r) for r in batch)       # one price per line
    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))   # 1. save the file
    consumer.commit(asynchronous=False)                 # 2. THEN tell Kafka "done"
    print(f"Saved {len(batch)} records -> {BUCKET}/{key}")


def main():
    print(f"Reading from topic '{TOPIC}' and writing to MinIO bucket '{BUCKET}'...")
    batch, last_write = [], time.time()

    while True:
        msg = consumer.poll(1.0)

        if msg is not None:
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            try:
                record = json.loads(msg.value())
            except json.JSONDecodeError:
                print(f"Skipping bad message at partition {msg.partition()} offset {msg.offset()}")
                continue
            # partition + offset uniquely identify a message; used later to remove duplicates
            record["kafka_partition"] = msg.partition()
            record["kafka_offset"] = msg.offset()
            batch.append(record)

        if batch and (len(batch) >= BATCH_SIZE or time.time() - last_write >= BATCH_SECONDS):
            save_batch(batch)
            batch, last_write = [], time.time()


if __name__ == "__main__":
    try:
        main()
    finally:
        consumer.close()
