-- Quiet init chatter a bit (optional)
SET client_min_messages = WARNING;

-------------------------------------------------------------------------------
-- Create extra databases (values come from custom GUCs set via -c app.*=...)
-- IMPORTANT: We avoid DO/transaction for CREATE DATABASE by using \gexec.
-------------------------------------------------------------------------------

-- APP DB (if app.app_db is set)
SELECT format('CREATE DATABASE %I', app_db)
FROM (SELECT current_setting('app.app_db', true) AS app_db) s
WHERE app_db IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = app_db)
\gexec

-- AIRFLOW DB (if app.airflow_db is set)
SELECT format('CREATE DATABASE %I', af_db)
FROM (SELECT current_setting('app.airflow_db', true) AS af_db) s
WHERE af_db IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = af_db)
\gexec

-------------------------------------------------------------------------------
-- Debezium role with REPLICATION attribute
-- Reads from app.debezium_user / app.debezium_password; falls back to defaults.
-------------------------------------------------------------------------------
DO $$
DECLARE
  dbz_user TEXT := COALESCE(current_setting('app.debezium_user', true), 'debezium');
  dbz_pass TEXT := COALESCE(current_setting('app.debezium_password', true), 'debezium');
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = dbz_user) THEN
    EXECUTE format('CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L', dbz_user, dbz_pass);
  END IF;
END$$;

-------------------------------------------------------------------------------
-- Marquez DB & user (role can be created in a DO; DB must be created outside)
-------------------------------------------------------------------------------

-- Ensure role "marquez" exists
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'marquez') THEN
    CREATE ROLE marquez LOGIN PASSWORD 'marquez';
  END IF;
END$$;

-- Ensure database "marquez" exists (outside DO)
SELECT 'CREATE DATABASE ' || quote_ident('marquez') || ' OWNER ' || quote_ident('marquez')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'marquez')
\gexec

-- Grant privileges on the database to the role
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;

-- (Optional) You can set a default schema/privileges here if needed once connected to marquez.
