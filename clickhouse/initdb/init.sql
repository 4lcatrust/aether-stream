/* operational */
CREATE DATABASE IF NOT EXISTS aether;
CREATE TABLE IF NOT EXISTS aether._healthcheck
(
  ts DateTime DEFAULT now(),
  msg String
)
ENGINE = MergeTree
ORDER BY ts;
INSERT INTO aether._healthcheck (msg) VALUES ('clickhouse up');

/* bronze layer */
CREATE DATABASE IF NOT EXISTS bronze;

CREATE OR REPLACE TABLE bronze.market_caps
(
    assetId String,
    symbol String,
    coinName String,
    currency String,
    marketCap Decimal(38, 18),
    circulatingSupply Decimal(38, 18),
    totalSupply Decimal(38, 18),
    maxSupply Decimal(38, 18),
    eventTimeMs UInt64,
    sourceTsMs UInt64,
    createdAtMs UInt64,
    updatedAtMs UInt64,
    op String,
    lsn UInt64,
    sourceDb String,
    sourceSchema String,
    sourceTable String,
    ingestion_date Date,
    hour String
)
ENGINE = S3(
  'http://minio:9000/bronze/market_caps/*/*/*.parquet',
  'minioadmin',
  'minioadmin',
  'Parquet'
)
SETTINGS use_hive_partitioning = 1;

CREATE OR REPLACE TABLE bronze.market_prices
(
    assetId String,
    symbol String,
    coinName String,
    currency String,
    price Decimal(38, 18),
    marketCap Decimal(38, 18),
    volume24h Decimal(38, 18),
    coinImage String,
    eventTimeMs UInt64,
    sourceTsMs UInt64,
    createdAtMs UInt64,
    updatedAtMs UInt64,
    op String,
    lsn UInt64,
    sourceDb String,
    sourceSchema String,
    sourceTable String,
    ingestion_date Date,
    hour String
)
ENGINE = S3(
  'http://minio:9000/bronze/market_prices/*/*/*.parquet',
  'minioadmin',
  'minioadmin',
  'Parquet'
)
SETTINGS use_hive_partitioning = 1;

/* silver layer */
CREATE DATABASE IF NOT EXISTS silver;

/* gold layer */
CREATE DATABASE IF NOT EXISTS gold;