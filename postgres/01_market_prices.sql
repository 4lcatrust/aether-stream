DROP TABLE IF EXISTS public.market_prices;

CREATE TABLE public.market_prices (
    asset_id        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    coin_name       TEXT NOT NULL,
    currency        TEXT NOT NULL,
    price           NUMERIC(18,8) NOT NULL,
    market_cap      NUMERIC(18,2),
    volume_24h      NUMERIC(18,2),
    coin_image      TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    source_ts       TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, currency, event_time)
);

CREATE INDEX idx_market_prices_event_time
ON public.market_prices (event_time);

ALTER TABLE public.market_prices
SET (autovacuum_vacuum_scale_factor = 0.05);