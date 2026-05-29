#!/usr/bin/env python3
import os, logging, requests, psycopg2, signal, time, random
from datetime import datetime, timezone
from psycopg2.extras import DictCursor
# ============================================================
# Configuration
# ============================================================
API_URL                 = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS                  = {
                            "vs_currency": "usd",
                            "order": "market_cap_desc",
                            "per_page": 10,
                            "page": 1,
                            "sparkline": "false",
                        }
STATEMENT_TIMEOUT       = "10s"
POLL_INTERVAL           = int(os.environ.get("POLL_INTERVAL", "60"))
MAX_API_RETRIES         = 5
INITIAL_BACKOFF         = 2
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

def load_db_config() -> dict:
    return {
        "host":     os.environ["PGHOST"],
        "port":     int(os.environ.get("PGPORT", "5432")),
        "dbname":   os.environ["PGDATABASE"],
        "user":     os.environ["PGUSER"],
        "password": os.environ["PGPASSWORD"],
    }

# ============================================================
# Single Execution Cycle (your original logic)
# ============================================================
def run_once(conn):
    log.info("Fetching market data from CoinGecko")
    backoff = INITIAL_BACKOFF
    assets = None
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            resp = requests.get(API_URL, params=PARAMS, timeout=10)
            if resp.status_code == 429:
                raise requests.HTTPError("Rate limited (429)")
            resp.raise_for_status()
            assets = resp.json()
            break
        except Exception as e:
            log.warning(
                "API attempt %d failed: %s | retrying in %ds",
                attempt,
                str(e),
                backoff,
            )
            time.sleep(backoff + random.uniform(0,1))
            backoff *= 2
    if assets is None:
        raise RuntimeError("API failed after max retries")
    if not assets:
        log.warning("No assets returned from API")
        return
    with conn.cursor() as cur:
        for asset in assets:
            asset_id = asset["id"]
            symbol = asset["symbol"]
            coin_name = asset["name"]
            event_time = parse_market_ts(asset["last_updated"])
            ingestion_time = datetime.now(timezone.utc)
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
    log.info("Cycle completed | assets=%d", len(assets))
# ============================================================
# Continuous Loop
# ============================================================
def main():
    log.info("Starting continuous ingestion service")
    db_config = load_db_config()
    conn = None
    while RUNNING:
        try:
            if conn is None or conn.closed:
                log.info("Opening PostgreSQL connection...")
                conn = psycopg2.connect(**db_config)
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}';")
            run_once(conn)
        except psycopg2.OperationalError:
            log.exception("Database connection lost. Reconnecting...")
            if conn:
                conn.close()
            conn = None
            time.sleep(5)
        except Exception:
            log.exception("Cycle failed")
        if RUNNING:
            time.sleep(POLL_INTERVAL)
    if conn and not conn.closed:
        conn.close()
    log.info("Ingestion service stopped cleanly")

if __name__ == "__main__":
    main()