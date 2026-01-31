CREATE DATABASE IF NOT EXISTS aether;

CREATE TABLE IF NOT EXISTS aether._healthcheck
(
  ts DateTime DEFAULT now(),
  msg String
)
ENGINE = MergeTree
ORDER BY ts;

INSERT INTO aether._healthcheck (msg) VALUES ('clickhouse up');

CREATE DATABASE IF NOT EXISTS bronze;

CREATE OR REPLACE VIEW bronze.market_caps AS
SELECT
  * EXCEPT(ingestionDate, ingestionHour),
  ingestionDate,
  ingestionHour
FROM s3(
  'http://minio:9000/bronze/market_caps/ingestion_date=*/hour=*/*.parquet',
  'minioadmin',
  'minioadmin',
  'Parquet'
)
SETTINGS use_hive_partitioning = 0;

CREATE OR REPLACE VIEW bronze.market_prices AS
SELECT
  * EXCEPT(ingestionDate, ingestionHour),
  ingestionDate,
  ingestionHour
FROM s3(
  'http://minio:9000/bronze/market_prices/ingestion_date=*/hour=*/*.parquet',
  'minioadmin',
  'minioadmin',
  'Parquet'
)
SETTINGS use_hive_partitioning = 0;
