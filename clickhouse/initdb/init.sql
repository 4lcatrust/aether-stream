/* operational */
CREATE DATABASE IF NOT EXISTS omt;
CREATE TABLE IF NOT EXISTS omt.healthcheck
(
  ts DateTime DEFAULT now(),
  msg String
)
ENGINE = MergeTree
ORDER BY ts;
INSERT INTO omt.healthcheck (msg) VALUES ('clickhouse up');

/* lz layer */
CREATE DATABASE IF NOT EXISTS lz;
CREATE OR REPLACE TABLE lz.market_caps
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

CREATE OR REPLACE TABLE lz.market_prices
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
    ingestion_hour String
)
ENGINE = MergeTree
PARTITION BY ingestion_date
ORDER BY (assetId, lsn);

CREATE OR REPLACE TABLE bronze.market_prices
(
    assetId String,
    symbol String,
    coinName String,
    currency String,
    price Decimal(38, 18),
    marketCap Decimal(38, 18),
    volume24h Decimal(38, 18),
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
    ingestion_hour String
)
ENGINE = MergeTree
PARTITION BY ingestion_date
ORDER BY (assetId, lsn);

/* silver layer */
CREATE DATABASE IF NOT EXISTS silver;
CREATE OR REPLACE TABLE silver.market_caps
(
    assetId String,
    state AggregateFunction(
      argMax,
        Tuple(
          symbol String,
          coinName String,
          currency String,
          marketCap Decimal(38, 18),
          circulatingSupply Decimal(38, 18),
          totalSupply Decimal(38, 18),
          maxSupply Decimal(38, 18),
          eventTimeMs UInt64,
          op String
        ),
      UInt64
    )
)
ENGINE = AggregatingMergeTree
ORDER BY (assetId);

DROP VIEW IF EXISTS silver.mv_market_caps;
CREATE MATERIALIZED VIEW silver.mv_market_caps
TO silver.market_caps
AS
SELECT
    assetId,
    argMaxState(
        tuple(
          symbol,
          coinName,
          currency,
          marketCap,
          circulatingSupply,
          totalSupply,
          maxSupply,
          eventTimeMs,
          op
        ),
        lsn
    ) AS state
FROM bronze.market_caps
GROUP BY assetId;

CREATE OR REPLACE VIEW silver.vw_market_caps AS
SELECT
    assetId AS asset_id,
    tupleElement(final_state, 1) AS symbol,
    tupleElement(final_state, 2) AS coin_name,
    tupleElement(final_state, 3) AS currency,
    tupleElement(final_state, 4) AS market_cap,
    tupleElement(final_state, 5) AS circulating_supply,
    tupleElement(final_state, 6) AS total_supply,
    tupleElement(final_state, 7) AS max_supply,
    tupleElement(final_state, 8) AS event_time_ms,
    toDateTime(tupleElement(final_state, 8) / 1000, 'Asia/Jakarta') AS event_time
FROM (
  SELECT
      assetId,
      argMaxMerge(state) AS final_state
  FROM silver.market_caps
  GROUP BY assetId
)
WHERE tupleElement(final_state, 9) != 'd'
;

CREATE OR REPLACE TABLE silver.market_prices
(
    assetId String,
    state AggregateFunction(
      argMax,
        Tuple(
          symbol String,
          coinName String,
          currency String,
          price Decimal(38, 18),
          marketCap Decimal(38, 18),
          volume24h Decimal(38, 18),
          eventTimeMs UInt64,
          op String
        ),
      UInt64
    )
)
ENGINE = AggregatingMergeTree
ORDER BY (assetId);

DROP VIEW IF EXISTS silver.mv_market_prices;
CREATE MATERIALIZED VIEW silver.mv_market_prices
TO silver.market_prices
AS
SELECT
    assetId,
    argMaxState(
        tuple(
          symbol,
          coinName,
          currency,
          price,
          marketCap,
          volume24h,
          eventTimeMs,
          op
        ),
        lsn
    ) AS state
FROM bronze.market_prices
GROUP BY assetId;

CREATE OR REPLACE VIEW silver.vw_market_prices AS
SELECT
    assetId AS asset_id,
    tupleElement(final_state, 1) AS symbol,
    tupleElement(final_state, 2) AS coin_name,
    tupleElement(final_state, 3) AS currency,
    tupleElement(final_state, 4) AS price,
    tupleElement(final_state, 5) AS market_cap,
    tupleElement(final_state, 6) AS volume_24h,
    tupleElement(final_state, 7) AS event_time_ms,
    toDateTime(tupleElement(final_state, 7) / 1000, 'Asia/Jakarta') AS event_time
FROM (
  SELECT
      assetId,
      argMaxMerge(state) AS final_state
  FROM silver.market_prices
  GROUP BY assetId
)
WHERE tupleElement(final_state, 8) != 'd'
;

/* gold layer */
CREATE DATABASE IF NOT EXISTS gold;