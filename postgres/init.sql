-- Create extra databases
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_database WHERE datname = current_setting('APP_DB', true)) THEN
    EXECUTE format('CREATE DATABASE %I', current_setting('APP_DB', true));
  END IF;

  IF NOT EXISTS (SELECT FROM pg_database WHERE datname = current_setting('AIRFLOW_DB', true)) THEN
    EXECUTE format('CREATE DATABASE %I', current_setting('AIRFLOW_DB', true));
  END IF;
END$$;

-- Create Debezium role with replication
DO $$
DECLARE
  dbz_user TEXT := current_setting('DEBEZIUM_USER', true);
  dbz_pass TEXT := current_setting('DEBEZIUM_PASSWORD', true);
BEGIN
  IF dbz_user IS NULL OR dbz_pass IS NULL THEN
    RAISE NOTICE 'DEBEZIUM env not set, using defaults';
    dbz_user := 'debezium';
    dbz_pass := 'debezium';
  END IF;

  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = dbz_user) THEN
    EXECUTE format('CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L', dbz_user, dbz_pass);
  END IF;
END$$;
