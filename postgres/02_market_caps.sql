DROP TABLE IF EXISTS public.market_caps;

CREATE TABLE public.market_caps (
    asset_id           TEXT NOT NULL,
    symbol             TEXT NOT NULL,
    coin_name          TEXT NOT NULL,
    currency           TEXT NOT NULL,
    market_cap         NUMERIC(18,2),
    circulating_supply NUMERIC(20,4),
    total_supply       NUMERIC(20,4),
    max_supply         NUMERIC(20,4),
    event_time         TIMESTAMPTZ NOT NULL,
    source_ts          TIMESTAMPTZ NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, currency, event_time)
);

CREATE INDEX idx_market_caps_event_time
ON public.market_caps (event_time);

ALTER TABLE public.market_caps
SET (autovacuum_vacuum_scale_factor = 0.05);