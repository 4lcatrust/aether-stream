-- ============================================================
-- Market Caps (CDC Source Table)
-- ============================================================
CREATE TABLE IF NOT EXISTS public.market_caps (
    asset_id                TEXT NOT NULL,
    symbol                  TEXT NOT NULL,
    coin_name               TEXT NOT NULL,
    currency                TEXT NOT NULL,
    market_cap              NUMERIC(18,2),
    circulating_supply      NUMERIC(20,4),
    total_supply            NUMERIC(20,4),
    max_supply              NUMERIC(20,4),
    event_time              TIMESTAMPTZ NOT NULL,
    source_ts               TIMESTAMPTZ NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT market_caps_pk PRIMARY KEY (asset_id, currency)
);

CREATE OR REPLACE FUNCTION public.set_updated_at_market_caps()
RETURNS TRIGGER AS $$
BEGIN
    IF (ROW(NEW.*) IS DISTINCT FROM ROW(OLD.*)) THEN
        NEW.updated_at = now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_market_caps_updated_at
BEFORE UPDATE ON public.market_caps
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at_market_caps();

CREATE INDEX IF NOT EXISTS idx_market_caps_updated_at
ON public.market_caps (updated_at);

ALTER TABLE public.market_caps REPLICA IDENTITY FULL;
ALTER TABLE public.market_caps SET (autovacuum_vacuum_scale_factor = 0.05);