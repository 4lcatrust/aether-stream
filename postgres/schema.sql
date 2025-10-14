-- 1) Create the DB if it's set and missing (safe to re-run)
SELECT format('CREATE DATABASE %I', app_db)
FROM (SELECT current_setting('app.app_db', true) AS app_db) s
WHERE app_db IS NOT NULL AND app_db <> ''
  AND NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = app_db)
\gexec

-- 2) Capture both the name and a boolean flag into psql variables
SELECT
  current_setting('app.app_db', true)                         AS app_db,
  (current_setting('app.app_db', true) IS NOT NULL
   AND current_setting('app.app_db', true) <> '')::boolean    AS has_app_db
\gset

-- 3) Only \connect if the flag is true
\if :has_app_db
\connect :app_db
\echo Connected to :app_db
\else
\echo Skipping connection to app.app_db not set
\endif

-- 4) Your DDL goes below; it will run in the app DB if connected,
--    otherwise it stays in the default 'postgres' DB.
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS indodax;

DO $$
DECLARE
  dbz_user TEXT := COALESCE(current_setting('app.debezium_user', true), 'debezium');
  public_schema TEXT := 'public';
  indodax_schema TEXT := 'indodax';
BEGIN
  -- grant connect to this DB
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), dbz_user);
  -- usage on schemas
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', public_schema, dbz_user);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', indodax_schema, dbz_user);
  -- select on existing tables
  EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', public_schema, dbz_user);
  EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', indodax_schema, dbz_user);

  -- default privileges for future tables (must be run by the *owner* that will create tables)
  EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO %I', dbz_user, public_schema, dbz_user);
  EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO %I', dbz_user, indodax_schema, dbz_user);
END$$;

/* Ticker table for Indodax PEPE/IDR market data */
CREATE TABLE IF NOT EXISTS indodax.ticker_pepeidr (
  id            BIGSERIAL PRIMARY KEY,
  server_time   BIGINT,
  last_price    DOUBLE PRECISION,
  high          DOUBLE PRECISION,
  low           DOUBLE PRECISION,
  vol_base      NUMERIC,
  vol_quote     NUMERIC,
  raw_payload   JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ticker_pepeidr_created_at
  ON indodax.ticker_pepeidr (created_at);

/* Ticker table for Indodax BTC/IDR market data */
CREATE TABLE IF NOT EXISTS indodax.ticker_btcidr (
  id            BIGSERIAL PRIMARY KEY,
  server_time   BIGINT,
  last_price    DOUBLE PRECISION,
  high          DOUBLE PRECISION,
  low           DOUBLE PRECISION,
  vol_base      NUMERIC,
  vol_quote     NUMERIC,
  raw_payload   JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ticker_pepeidr_created_at
  ON indodax.ticker_btcidr (created_at);