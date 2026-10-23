"""
Step 5: Loader - moves new files from MinIO into Snowflake.
Run:  python loader.py   (safe to run many times - never loads the same file twice)
"""

import os
import tempfile

import boto3
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")
BUCKET = "bronze"
PREFIX = "stock_quotes/"


def snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="RAW",
        authenticator="SNOWFLAKE_JWT",                            # key-pair authentication
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        session_parameters={"TIMEZONE": "UTC"},                   # store timestamps in UTC (trial accounts default to LA time)
    )


def load():
    s3 = boto3.client(                                            # MinIO client (S3-compatible)
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    conn = snowflake_connection()
    cur = conn.cursor()
    try:
        # 1. Find the newest file already in Snowflake (file names sort by time)
        cur.execute("SELECT MAX(SOURCE_FILE) FROM STOCK_QUOTES")
        last = cur.fetchone()[0] or ""
        print(f"Last loaded file: {last or '(none)'}")

        # 2. List only the MinIO files newer than that
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=PREFIX, StartAfter=last or PREFIX
        )
        keys = [obj["Key"] for page in pages for obj in page.get("Contents", [])]
        print(f"New files in MinIO: {len(keys)}")
        if not keys:
            return 0

        # 3. Download each file and upload it to Snowflake's upload area (stage)
        # from MinIO into a temporary folder on your laptop.
        with tempfile.TemporaryDirectory() as tmp:
            for key in keys:
                local = os.path.join(tmp, os.path.basename(key)).replace(os.sep, "/")
                s3.download_file(BUCKET, key, local)
                cur.execute(f"PUT 'file://{local}' @BRONZE_STAGE/{os.path.dirname(key)}/")

        # 4. Copy the uploaded files into the table, then clear the upload area.
        #    Snowflake skips files it already loaded, so no duplicates.
        cur.execute("""
            COPY INTO STOCK_QUOTES (RECORD, SOURCE_FILE)
            FROM (SELECT $1, REPLACE(METADATA$FILENAME, '.gz', '') FROM @BRONZE_STAGE)
            FILE_FORMAT = (FORMAT_NAME = 'JSON_LINES')
            PURGE = TRUE
        """)
        rows = sum(r[3] for r in cur.fetchall() if len(r) > 1)   # r[3] = rows loaded per file
        print(f"Loaded {rows} rows into STOCK_QUOTES")
        return rows
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load()
