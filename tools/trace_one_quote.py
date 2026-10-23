"""
Follow ONE quote through every stage of the pipeline:
Kafka -> MinIO -> Snowflake RAW -> BRONZE -> SILVER -> GOLD.

Picks the newest quote for the stock that has already reached SILVER
(i.e. Airflow has loaded it and dbt has processed it).

Run from the project root:
    venv\\Scripts\\python tools/trace_one_quote.py          (AAPL)
    venv\\Scripts\\python tools/trace_one_quote.py TSLA
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)                          # so relative paths in .env (keys/...) work
sys.path.insert(0, str(ROOT))           # so we can import loader.py
sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import boto3
from confluent_kafka import Consumer, TopicPartition
from loader import snowflake_connection

SYMBOL = (sys.argv[1] if len(sys.argv) > 1 else "AAPL").upper()

cur = snowflake_connection().cursor()


def q(sql):
    cur.execute(sql)
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def show(title, data):
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")
    print(data)


# Pick the newest quote for this stock that has made it all the way to SILVER
rows = q(f"""select * from silver.silver_stock_quotes where symbol = '{SYMBOL}'
             order by ingested_at desc limit 1""")
if not rows:
    sys.exit(f"No {SYMBOL} quotes in SILVER yet. Is the symbol right, and has the pipeline run?")
s = rows[0]
part, off = map(int, s["quote_id"].split("-"))
print(f"Tracing {SYMBOL} quote: Kafka partition {part}, offset {off}")

# STOP 1: Kafka (read that exact message by its address; no offsets are committed)
c = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": "trace-tool",
    "enable.auto.commit": False,
})
c.assign([TopicPartition(os.getenv("KAFKA_TOPIC", "stock-quotes"), part, off)])
m = c.poll(10)
c.close()
if m is None or m.error():
    show("STOP 1 · KAFKA", "(message no longer in Kafka - it keeps messages for 7 days)")
else:
    show(f"STOP 1 · KAFKA  (partition {part}, offset {off}, key = {m.key().decode()})", m.value().decode())

# STOP 2: MinIO (find the file via SOURCE_FILE, then the line inside it)
raw = q(f"""select source_file, loaded_at, record from raw.stock_quotes
            where record:kafka_partition = {part} and record:kafka_offset = {off}""")[0]
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password123"),
    region_name="us-east-1",
)
lines = s3.get_object(Bucket="bronze", Key=raw["source_file"])["Body"].read().decode().splitlines()
line_no, line = next(
    (i, l) for i, l in enumerate(lines, 1)
    if json.loads(l)["kafka_partition"] == part and json.loads(l)["kafka_offset"] == off
)
show(f"STOP 2 · MINIO  (bucket bronze, {raw['source_file']}, line {line_no} of {len(lines)})", line)

# STOP 3: Snowflake RAW
show("STOP 3 · SNOWFLAKE RAW.STOCK_QUOTES  (whole JSON in one VARIANT column)",
     f"RECORD      = {json.dumps(json.loads(raw['record']))}\n"
     f"SOURCE_FILE = {raw['source_file']}\n"
     f"LOADED_AT   = {raw['loaded_at']} UTC")

# STOP 4: BRONZE
b = q(f"select * from bronze.bronze_stock_quotes where kafka_partition = {part} and kafka_offset = {off}")[0]
show("STOP 4 · BRONZE.BRONZE_STOCK_QUOTES  (JSON split into typed columns)",
     "\n".join(f"{k:16} = {v}" for k, v in b.items() if k != "source_file"))

# STOP 5: SILVER
show("STOP 5 · SILVER.SILVER_STOCK_QUOTES  (deduplicated, New York time added)",
     "\n".join(f"{k:16} = {v}" for k, v in s.items()))

# STOP 6: GOLD
interval = q(f"""select * from gold.gold_prices_5min where symbol = '{SYMBOL}'
               and interval_start = time_slice('{s['quote_at_ny']}'::timestamp_ntz, 5, 'MINUTE')""")
day = q(f"""select symbol, trading_date, open_price, high_price, low_price, close_price,
                   prev_close, daily_return_pct, num_quotes
            from gold.gold_daily_summary
            where symbol = '{SYMBOL}' and trading_date = '{s['trading_date']}'""")
show("STOP 6 · GOLD  (this quote is now one of many inside a summary)",
     "gold_prices_5min   → " + (", ".join(f"{k}={v}" for k, v in interval[0].items()) if interval else "(none)")
     + "\ngold_daily_summary → " + (", ".join(f"{k}={v}" for k, v in day[0].items()) if day else "(none)"))
