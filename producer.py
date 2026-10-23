"""
Step 2: Producer - gets stock prices from Finnhub and sends them to Kafka.
Every 15 seconds: ask for each stock's price, send it to Kafka if it changed.
Run:  python producer.py
"""
import json
import os
import sys
import time
from datetime import datetime, timezone
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY", "")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "AAPL,MSFT").split(",") if s.strip()]
INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))

QUOTE_URL = "https://finnhub.io/api/v1/quote"


def fetch_quote(session, symbol):
    resp = session.get(QUOTE_URL, params={"symbol": symbol, "token": API_KEY}, timeout=10)
    resp.raise_for_status()                 # stop if Finnhub says no (bad key, too many calls)
    q = resp.json()
    if not q or q.get("t", 0) == 0:         # unknown stock: Finnhub sends all zeros
        return None
    return {                                # rename Finnhub's short names to readable ones
        "symbol": symbol,
        "price": q["c"],
        "change": q["d"],
        "change_pct": q["dp"],
        "high": q["h"],
        "low": q["l"],
        "open": q["o"],
        "prev_close": q["pc"],
        "quote_ts": q["t"],                 # time of the price, in seconds
        "ingested_at": datetime.now(timezone.utc).isoformat(),   # when we fetched it
    }


def on_delivery(err, msg):                  # Kafka calls this for every message
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}", file=sys.stderr)


def main():
    if not API_KEY or API_KEY == "your_finnhub_api_key_here":   # no key: stop right away
        sys.exit("FINNHUB_API_KEY is missing. Put your key in the .env file.")

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "finnhub-producer"})
    session = requests.Session()            # one open connection to Finnhub, reused
    last_sent = {}                          # last price time sent per stock, to skip repeats
    print(f"Producing {SYMBOLS} to '{TOPIC}' on {BOOTSTRAP} every {INTERVAL}s. Ctrl+C to stop.")

    try:
        while True:
            for symbol in SYMBOLS:
                try:
                    quote = fetch_quote(session, symbol)
                except requests.RequestException as e:   # network problem: skip this stock
                    print(f"Error fetching {symbol}: {e}", file=sys.stderr)
                    continue
                if quote is None:
                    print(f"No data for {symbol}", file=sys.stderr)
                    continue
                if last_sent.get(symbol) == quote["quote_ts"]:
                    continue                # same price as last time: nothing new
                producer.produce(TOPIC, key=symbol, value=json.dumps(quote), on_delivery=on_delivery)
                producer.poll(0)            # check for delivery receipts
                last_sent[symbol] = quote["quote_ts"]
                print(f"Sent {symbol}: {quote['price']}")
            producer.flush(10)              # wait until Kafka has every message
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush(10)                  # deliver anything left before quitting


if __name__ == "__main__":
    main()
