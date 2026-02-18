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
CREATE OR REPLACE TABLE gold.dim_asset
(
    asset_id String,
    symbol String,
    coin_name String,
    coin_image String,
    currency String,
    last_event_time_ms UInt64,
    last_event_time DateTime('Asia/Jakarta'),
    last_lsn UInt64,
    last_op String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_lsn)
ORDER BY (asset_id);

DROP VIEW IF EXISTS gold.mv_dim_asset_from_prices;
CREATE MATERIALIZED VIEW gold.mv_dim_asset_from_prices
TO gold.dim_asset
AS
SELECT
    assetId AS asset_id,
    symbol AS symbol,
    coinName AS coin_name,
    coinImage AS coin_image,
    currency AS currency,
    eventTimeMs AS last_event_time_ms,
    toDateTime(eventTimeMs / 1000, 'Asia/Jakarta') AS last_event_time,
    lsn AS last_lsn,
    op  AS last_op,
    now() AS updated_at
FROM bronze.market_prices
WHERE op != 'd';

CREATE OR REPLACE TABLE gold.fct_market_prices
(
    asset_id String,
    event_time_ms UInt64,
    event_time DateTime('Asia/Jakarta'),
    event_date Date MATERIALIZED toDate(event_time),
    price Decimal(38, 18),
    market_cap Decimal(38, 18),
    volume_24h Decimal(38, 18),
    currency String,
    lsn UInt64,
    op String,
    ingestion_date Date,
    ingestion_hour String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(lsn)
PARTITION BY toYYYYMM(event_time)
ORDER BY (asset_id, event_time_ms);

DROP VIEW IF EXISTS gold.mv_fct_market_prices;
CREATE MATERIALIZED VIEW gold.mv_fct_market_prices
TO gold.fct_market_prices
AS
SELECT
    assetId AS asset_id,
    eventTimeMs AS event_time_ms,
    toDateTime(eventTimeMs / 1000, 'Asia/Jakarta') AS event_time,
    price AS price,
    marketCap AS market_cap,
    volume24h AS volume_24h,
    currency AS currency,
    lsn,
    op,
    ingestion_date,
    ingestion_hour,
    now() AS loaded_at
FROM bronze.market_prices
WHERE op != 'd';

CREATE OR REPLACE TABLE gold.fct_market_caps
(
    asset_id String,
    event_time_ms UInt64,
    event_time DateTime('Asia/Jakarta'),
    event_date Date MATERIALIZED toDate(event_time),
    market_cap Decimal(38, 18),
    circulating_supply Decimal(38, 18),
    total_supply Decimal(38, 18),
    max_supply Decimal(38, 18),
    currency String,
    lsn UInt64,
    op String,
    ingestion_date Date,
    ingestion_hour String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(lsn)
PARTITION BY toYYYYMM(event_time)
ORDER BY (asset_id, event_time_ms);

DROP VIEW IF EXISTS gold.mv_fct_market_caps;
CREATE MATERIALIZED VIEW gold.mv_fct_market_caps
TO gold.fct_market_caps
AS
SELECT
    assetId AS asset_id,
    eventTimeMs AS event_time_ms,
    toDateTime(eventTimeMs / 1000, 'Asia/Jakarta') AS event_time,
    marketCap AS market_cap,
    circulatingSupply AS circulating_supply,
    totalSupply AS total_supply,
    maxSupply AS max_supply,
    currency,
    lsn,
    op,
    ingestion_date,
    ingestion_hour,
    now() AS loaded_at
FROM bronze.market_caps
WHERE op != 'd';

CREATE OR REPLACE VIEW gold.sem_market_overview
AS
WITH
latest_prices AS (
SELECT
    asset_id,
    argMax(price, event_time) AS price,
    argMax(volume_24h, event_time) AS volume_24h,
    argMax(market_cap, event_time) AS market_cap,
    max(event_time) AS last_event_time
FROM gold.fct_market_prices
GROUP BY asset_id
),
latest_caps AS (
SELECT
    asset_id,
    argMax(circulating_supply, event_time) AS circulating_supply,
    argMax(total_supply, event_time) AS total_supply,
    argMax(NULLIF(max_supply, -1), event_time) AS max_supply
FROM gold.fct_market_caps
GROUP BY asset_id
),
merged AS (
SELECT
    p.asset_id,
    p.price,
    p.volume_24h,
    p.market_cap,
    c.circulating_supply,
    c.total_supply,
    c.max_supply,
    p.last_event_time,
    SUM(p.market_cap::Float64) OVER() AS whole_market_cap,
    SUM(c.circulating_supply::Float64) OVER() AS whole_circulating_supply
FROM latest_prices p
LEFT JOIN latest_caps c
    ON p.asset_id = c.asset_id
)
SELECT
    m.* EXCEPT(
    	whole_market_cap,
    	whole_circulating_supply
	),
    RANK() OVER (ORDER BY market_cap DESC) AS market_cap_rank,
    volume_24h / market_cap::Float64 AS liquid_indicator_24h,
    market_cap / whole_market_cap AS market_dominance,
    circulating_supply / whole_circulating_supply AS supply_dominance
FROM merged m;