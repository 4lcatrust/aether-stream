-- ============================================================
-- Market Prices (CDC Source Table)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.market_prices (
    asset_id                TEXT NOT NULL,
    symbol                  TEXT NOT NULL,
    coin_name               VARCHAR NOT NULL,
    currency                TEXT NOT NULL,
    price                   NUMERIC(18,8) NOT NULL,
    market_cap              NUMERIC(18,2),
    volume_24h              NUMERIC(18,2),
    coin_image              VARCHAR NOT NULL,
    event_time              TIMESTAMPTZ NOT NULL,
    source_ts               TIMESTAMPTZ NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT market_prices_pk PRIMARY KEY (asset_id, currency)
);

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    IF (ROW(NEW.*) IS DISTINCT FROM ROW(OLD.*)) THEN
        NEW.updated_at = now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_market_prices_updated_at
BEFORE UPDATE ON public.market_prices
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_market_prices_updated_at
ON public.market_prices (updated_at);

ALTER TABLE public.market_prices REPLICA IDENTITY FULL;
ALTER TABLE public.market_prices SET (autovacuum_vacuum_scale_factor = 0.05);