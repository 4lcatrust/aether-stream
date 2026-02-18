#!/usr/bin/env python3

import logging
import requests
import psycopg2
import signal
import time
from datetime import datetime, timezone
from psycopg2.extras import DictCursor

# ============================================================
# Configuration
# ============================================================
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": "false",
}
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "aether",
    "user": "aether",
    "password": "aether",
}
STATEMENT_TIMEOUT = "10s"
POLL_INTERVAL = 15

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("market_ingest_service")

# ============================================================
# Shutdown Handling
# ============================================================
RUNNING = True
def shutdown_handler(signum, frame):
    global RUNNING
    log.info("Shutdown signal received. Stopping ingestion loop...")
    RUNNING = False
signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# ============================================================
# SQL
# ============================================================
UPSERT_MARKET_PRICES_SQL = """
INSERT INTO public.market_prices (
    asset_id,
    symbol,
    coin_name,
    currency,
    price,
    market_cap,
    volume_24h,
    coin_image,
    event_time,
    source_ts
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (asset_id, currency, event_time)
DO NOTHING;
"""

UPSERT_MARKET_CAPS_SQL = """
INSERT INTO public.market_caps (
    asset_id,
    symbol,
    coin_name,
    currency,
    market_cap,
    circulating_supply,
    total_supply,
    max_supply,
    event_time,
    source_ts
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (asset_id, currency, event_time)
DO NOTHING;
"""

# ============================================================
# Utilities
# ============================================================
def parse_market_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))

# ============================================================
# Single Execution Cycle (your original logic)
# ============================================================
def run_once():
    log.info("Fetching market data from CoinGecko")
    resp = requests.get(API_URL, params=PARAMS, timeout=10)
    resp.raise_for_status()
    assets = resp.json()
    if not assets:
        log.warning("No assets returned from API")
        return
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}';")
            for asset in assets:
                asset_id = asset["id"]
                symbol = asset["symbol"]
                coin_name = asset["name"]
                event_time = parse_market_ts(asset["last_updated"])
                ingestion_time = datetime.now(timezone.utc)
                # market_prices
                cur.execute(
                    UPSERT_MARKET_PRICES_SQL,
                    (
                        asset_id,
                        symbol,
                        coin_name,
                        "usd",
                        asset["current_price"],
                        asset.get("market_cap"),
                        asset.get("total_volume"),
                        asset["image"],
                        event_time,
                        ingestion_time,
                    ),
                )
                # market_caps
                cur.execute(
                    UPSERT_MARKET_CAPS_SQL,
                    (
                        asset_id,
                        symbol,
                        coin_name,
                        "usd",
                        asset.get("market_cap"),
                        asset.get("circulating_supply"),
                        asset.get("total_supply"),
                        asset.get("max_supply"),
                        event_time,
                        ingestion_time,
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Ingest failed; transaction rolled back")
        raise
    finally:
        conn.close()
    log.info("Cycle completed | assets=%d", len(assets))

# ============================================================
# Continuous Loop
# ============================================================
def main():
    log.info("Starting continuous ingestion service")
    while RUNNING:
        try:
            run_once()
        except Exception as e:
            log.error("Cycle failed: %s", e)
        if RUNNING:
            time.sleep(POLL_INTERVAL)
    log.info("Ingestion service stopped cleanly")
if __name__ == "__main__":
    main()