/* === Quiet init chatter a bit (optional) === */
SET client_min_messages = WARNING;

/* === APP DB (if app.app_db is set) === */
SELECT format('CREATE DATABASE %I', app_db)
FROM (SELECT current_setting('app.app_db', true) AS app_db) s
WHERE app_db IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = app_db)
\gexec

SELECT
  current_setting('app.app_db', true)                         AS app_db,
  (current_setting('app.app_db', true) IS NOT NULL
   AND current_setting('app.app_db', true) <> '')::boolean    AS has_app_db
\gset

/* === AIRFLOW DB (if app.airflow_db is set) === */
SELECT format('CREATE DATABASE %I', af_db)
FROM (SELECT current_setting('app.airflow_db', true) AS af_db) s
WHERE af_db IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = af_db)
\gexec

/* === DEBEZIUM ROLE === */
DO $$
DECLARE
  dbz_user TEXT := COALESCE(current_setting('app.debezium_user', true), 'debezium');
  dbz_pass TEXT := COALESCE(current_setting('app.debezium_password', true), 'debezium');
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = dbz_user) THEN
    EXECUTE format('CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L', dbz_user, dbz_pass);
  END IF;
END$$;

/* === Marquez DB & user (role can be created in a DO; DB must be created outside) === */
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'marquez') THEN
    CREATE ROLE marquez LOGIN PASSWORD 'marquez';
  END IF;
END$$;

SELECT 'CREATE DATABASE ' || quote_ident('marquez') || ' OWNER ' || quote_ident('marquez')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'marquez')
\gexec

GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;