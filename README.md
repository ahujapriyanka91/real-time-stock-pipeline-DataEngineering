# Real-Time Stock Market Data Pipeline

An end-to-end data pipeline that streams live stock prices for 7 companies, stores the raw data in a data lake, loads it into Snowflake every hour, transforms it with dbt into bronze / silver / gold layers, and shows the results on a dashboard.

**Stack:** Python · Kafka · MinIO (S3) · Snowflake · dbt · Airflow · Docker · Streamlit



## How it works

| Step | Component | What it does |
|---|---|---|
| 1 | `producer.py` | Requests the latest quote for each stock from the Finnhub API every 15 seconds. Sends a quote to Kafka only when it has changed, keyed by stock symbol. |
| 2 | Kafka | Stores the messages durably (topic `stock-quotes`, 3 partitions, 7-day retention), decoupling ingestion from storage. |
| 3 | `consumer.py` | Reads from Kafka, adds each message's partition and offset, and writes batches (50 messages or 60 seconds) as JSON Lines files to MinIO. Offsets are committed only after a file is saved. |
| 4 | MinIO | S3-compatible object storage acting as the raw data lake (`bronze/stock_quotes/date=YYYY-MM-DD/`). |
| 5 | `loader.py` | Lists only files newer than the last one loaded, uploads them to a Snowflake internal stage, and runs `COPY INTO` the raw table. |
| 6 | dbt | Builds bronze, silver and gold models in Snowflake and runs data quality tests. |
| 7 | Airflow | Runs the loader and then dbt every hour at :05, 10:05–17:05 New York time, Monday–Friday, with retries. |
| 8 | Streamlit | Reads the gold tables and shows latest prices, daily returns and intraday price movement. |

## Data model (dbt)

| Layer | Model | Description |
|---|---|---|
| Source | `RAW.STOCK_QUOTES` | One row per message, full JSON in a `VARIANT` column, plus source file and load time. |
| Bronze | `bronze_stock_quotes` (view) | JSON parsed into typed columns. |
| Silver | `silver_stock_quotes` (incremental) | Deduplicated on Kafka partition + offset, invalid rows removed, New York market time and trading date added. Processes only new rows and merges on `quote_id`. |
| Gold | `gold_latest_quotes` | Latest price per stock with company name and sector. |
| Gold | `gold_daily_summary` | One row per stock per trading day: open, high, low, close, daily return %, intraday range %. |
| Gold | `gold_prices_5min` | Open, high, low and close per stock for each 5-minute interval. |
| Seed | `symbols` | Company name and sector for each ticker. |

**Tests (13):** `unique` and `not_null` on keys, referential integrity of symbols against the seed, a custom composite-uniqueness test on the gold tables, and a singular test that every 5-minute interval has valid price ranges. A source freshness check warns if no data has arrived in 6 hours.

## Design decisions

- **At-least-once delivery, exact deduplication.** The consumer commits Kafka offsets only after the file is written to MinIO, so a crash can cause re-reads but never data loss. Duplicates are removed in the silver layer using Kafka partition + offset as a natural unique key.
- **Idempotent loading.** The loader uses the latest loaded file name as a watermark (file names sort by time), and Snowflake's `COPY INTO` skips files it has already loaded. Running the loader twice loads nothing new.
- **Source-side filtering.** The producer skips quotes whose timestamp has not changed, so no redundant messages are sent while the market is closed.
- **Raw data kept as-is.** JSON is loaded into a `VARIANT` column and parsed in dbt, so source schema changes do not break loading, and every table can be rebuilt from the raw files.
- **Incremental models.** The silver model processes only rows loaded since the last run, keeping cost flat as data grows.
- **Cost control.** X-Small warehouse with 60-second auto-suspend, hourly batch runs during market hours only, dashboard query caching, and a resource monitor that suspends the warehouse at 30 credits per month.
- **Security.** A dedicated Snowflake service user with key-pair authentication and a least-privilege role. Secrets live in a git-ignored `.env` file.

## Project structure

```
├── docker-compose.yml        # all services: Kafka, MinIO, Airflow, producer, consumer, dashboard
├── Dockerfile                # image for producer, consumer and dashboard
├── producer.py               # Finnhub → Kafka
├── consumer.py               # Kafka → MinIO
├── loader.py                 # MinIO → Snowflake RAW
├── requirements.txt
├── .env.example              # settings template
├── snowflake/setup.sql       # one-time Snowflake setup
├── airflow/
│   ├── Dockerfile            # Airflow image with dbt and the Snowflake connector
│   └── dags/stock_pipeline.py
├── dbt_stocks/               # dbt project: models, tests, seed, macros
├── dashboard/app.py          # Streamlit dashboard
└── tools/trace_one_quote.py  # follows one quote through every stage
```

## Running it

**Prerequisites:** Docker Desktop, Python 3.12, a free [Finnhub](https://finnhub.io) API key, and a Snowflake account.

1. **Configure settings**
   ```bash
   cp .env.example .env
   ```
   Fill in `FINNHUB_API_KEY`, `SNOWFLAKE_ACCOUNT`, and two Airflow secrets:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # AIRFLOW_FERNET_KEY
   python -c "import secrets; print(secrets.token_urlsafe(32))"                               # AIRFLOW_JWT_SECRET
   ```

2. **Create a Snowflake key pair**
   ```bash
   mkdir keys
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/snowflake_rsa_key.p8 -nocrypt
   openssl rsa -in keys/snowflake_rsa_key.p8 -pubout -out keys/snowflake_rsa_key.pub
   ```

3. **Set up Snowflake:** paste your public key (without the header and footer lines) into `RSA_PUBLIC_KEY` in `snowflake/setup.sql`, then run the script in a Snowflake worksheet as `ACCOUNTADMIN`.

4. **Start everything**
   ```bash
   docker compose up -d
   ```
   Then enable the `stock_pipeline` DAG in Airflow.

| Service | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| Airflow | http://localhost:8080 |
| Kafka UI | http://localhost:8085 |
| MinIO console | http://localhost:9001 |

**Optional, for running parts by hand:**
```bash
python -m venv venv && venv\Scripts\activate      # Windows
pip install -r requirements.txt
python loader.py                                   # load new files into Snowflake
dotenv run -- dbt build                            # build and test the dbt models
python tools/trace_one_quote.py AAPL               # follow one quote through every stage
```

## What I would change for production

This project runs locally for learning and demonstration. In production I would:

- use managed services (e.g. Confluent Cloud or Amazon MSK for Kafka, Amazon S3 instead of MinIO)
- run Kafka with 3+ brokers and replication factor 3
- enable authentication on Kafka and Airflow, and keep secrets in a secrets manager
- pin all Docker images to exact versions and scan them in CI
- add CI that runs `dbt build` against a development schema on every pull request
- add alerting on failed Airflow runs and stale data
