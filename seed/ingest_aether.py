#!/usr/bin/env python3

import logging
import requests
import psycopg2
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
    "host": "localhost",        # use "postgres" when containerized
    "port": 5432,
    "dbname": "aether",
    "user": "aether",
    "password": "aether",
}

STATEMENT_TIMEOUT = "10s"

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("market_ingest")

# ============================================================
# SQL Statements
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
ON CONFLICT (asset_id, currency)
DO UPDATE SET
    price       = EXCLUDED.price,
    market_cap  = EXCLUDED.market_cap,
    volume_24h  = EXCLUDED.volume_24h,
    coin_image  = EXCLUDED.coin_image,
    event_time  = EXCLUDED.event_time,
    source_ts   = EXCLUDED.source_ts
RETURNING xmax;
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
ON CONFLICT (asset_id, currency)
DO UPDATE SET
    market_cap          = EXCLUDED.market_cap,
    circulating_supply  = EXCLUDED.circulating_supply,
    total_supply        = EXCLUDED.total_supply,
    max_supply          = EXCLUDED.max_supply,
    event_time          = EXCLUDED.event_time,
    source_ts           = EXCLUDED.source_ts
RETURNING xmax;
"""

# ============================================================
# Main
# ============================================================
def main() -> None:
    log.info("Fetching market data from CoinGecko")

    resp = requests.get(API_URL, params=PARAMS, timeout=10)
    resp.raise_for_status()
    assets = resp.json()

    if not assets:
        log.warning("No assets returned from API")
        return

    log.info("Fetched %d assets", len(assets))

    now = datetime.now(timezone.utc)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    price_inserted = 0
    price_updated = 0
    cap_inserted = 0
    cap_updated = 0

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Prevent infinite blocking on row / transaction locks
            cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}';")

            for asset in assets:
                asset_id = asset["id"]
                symbol = asset["symbol"]
                coin_name = asset["name"]

                # ------------------------------------------------
                # Table 1: market_prices
                # ------------------------------------------------
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
                        now,
                        asset["last_updated"],
                    ),
                )

                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"No RETURNING result for market_prices asset_id={asset_id}"
                    )

                if row["xmax"] == 0:
                    price_inserted += 1
                else:
                    price_updated += 1

                # ------------------------------------------------
                # Table 2: market_caps
                # ------------------------------------------------
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
                        now,
                        asset["last_updated"],
                    ),
                )

                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"No RETURNING result for market_caps asset_id={asset_id}"
                    )

                if row["xmax"] == 0:
                    cap_inserted += 1
                else:
                    cap_updated += 1

        conn.commit()

    except Exception:
        conn.rollback()
        log.exception("Ingest failed; transaction rolled back")
        raise

    finally:
        conn.close()

    log.info(
        "Ingest completed | assets=%d | "
        "market_prices(inserted=%d updated=%d) | "
        "market_caps(inserted=%d updated=%d)",
        len(assets),
        price_inserted,
        price_updated,
        cap_inserted,
        cap_updated,
    )

if __name__ == "__main__":
    main()