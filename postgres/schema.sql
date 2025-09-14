\connect indodax;

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS ticker_pepeidr (
  id            BIGSERIAL PRIMARY KEY,
  server_time   BIGINT,                  -- from API (unix seconds)
  last_price    DOUBLE PRECISION,
  high          DOUBLE PRECISION,
  low           DOUBLE PRECISION,
  vol_base      NUMERIC,                 -- e.g., vol_pepe
  vol_quote     NUMERIC,                 -- e.g., vol_idr
  raw_payload   JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Helpful index to time-window queries
CREATE INDEX IF NOT EXISTS idx_ticker_pepeidr_created_at ON ticker_pepeidr (created_at);
