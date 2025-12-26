-- ============================================================================
-- Crypto Data Pipeline - Star Schema Transformations
-- ============================================================================
-- Description : Builds analytics-friendly star schema tables and populates them
--               from the 3NF core tables.
-- Database    : PostgreSQL (compatible with Aurora / RDS / local Postgres)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Schema bootstrap
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics, public;

-- ---------------------------------------------------------------------------
-- 2. Dimension tables (DDL)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_cryptocurrency (
    crypto_key            SERIAL PRIMARY KEY,
    crypto_id             VARCHAR(50) NOT NULL UNIQUE,
    symbol                VARCHAR(20) NOT NULL,
    name                  VARCHAR(100) NOT NULL,
    image_url             VARCHAR(500),
    first_snapshot_at     TIMESTAMP,
    last_source_update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_cryptocurrency IS 'Conformed cryptocurrency dimension (Type 1)';

CREATE TABLE IF NOT EXISTS dim_source (
    source_key        SERIAL PRIMARY KEY,
    source_name       VARCHAR(100) NOT NULL UNIQUE,
    first_ingested_at TIMESTAMP NOT NULL,
    last_ingested_at  TIMESTAMP NOT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_source IS 'Ingestion source dimension (ETL lineage)';

CREATE TABLE IF NOT EXISTS dim_date (
    date_key       INTEGER PRIMARY KEY,
    full_date      DATE NOT NULL UNIQUE,
    day_of_month   SMALLINT NOT NULL,
    day_name       VARCHAR(10) NOT NULL,
    iso_week       SMALLINT NOT NULL,
    month_of_year  SMALLINT NOT NULL,
    month_name     VARCHAR(10) NOT NULL,
    quarter_of_year SMALLINT NOT NULL,
    calendar_year  INTEGER NOT NULL,
    is_weekend     BOOLEAN NOT NULL
);

COMMENT ON TABLE dim_date IS 'Calendar dimension derived from snapshot dates';

CREATE TABLE IF NOT EXISTS dim_time (
    time_key    INTEGER PRIMARY KEY,
    full_time   TIME NOT NULL UNIQUE,
    hour_24     SMALLINT NOT NULL,
    minute_of_hour SMALLINT NOT NULL,
    second_of_minute SMALLINT NOT NULL
);

COMMENT ON TABLE dim_time IS 'Time-of-day dimension at minute grain';

-- ---------------------------------------------------------------------------
-- 3. Fact table (DDL)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_crypto_price_metrics (
    fact_id                     BIGSERIAL PRIMARY KEY,
    crypto_key                  INTEGER NOT NULL REFERENCES dim_cryptocurrency(crypto_key),
    date_key                    INTEGER NOT NULL REFERENCES dim_date(date_key),
    time_key                    INTEGER NOT NULL REFERENCES dim_time(time_key),
    source_key                  INTEGER NOT NULL REFERENCES dim_source(source_key),
    batch_id                    INTEGER NOT NULL,
    snapshot_timestamp          TIMESTAMP NOT NULL,
    last_updated_at             TIMESTAMP NOT NULL,
    current_price               DECIMAL(20, 8) NOT NULL,
    high_24h                    DECIMAL(20, 8),
    low_24h                     DECIMAL(20, 8),
    price_change_24h            DECIMAL(20, 8),
    price_change_pct_24h        DECIMAL(10, 5),
    ath                         DECIMAL(20, 8),
    ath_change_pct              DECIMAL(10, 5),
    atl                         DECIMAL(20, 8),
    atl_change_pct              DECIMAL(10, 5),
    market_cap                  BIGINT,
    market_cap_rank             INTEGER,
    fully_diluted_valuation     BIGINT,
    total_volume                BIGINT,
    market_cap_change_24h       BIGINT,
    market_cap_change_pct_24h   DECIMAL(10, 5),
    circulating_supply          DECIMAL(30, 8),
    total_supply                DECIMAL(30, 8),
    max_supply                  DECIMAL(30, 8),
    roi_times                   DECIMAL(20, 8),
    roi_percentage              DECIMAL(20, 8),
    load_timestamp              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_crypto_snapshot UNIQUE (crypto_key, snapshot_timestamp)
);

COMMENT ON TABLE fact_crypto_price_metrics IS 'Star schema fact table at cryptocurrency + snapshot timestamp grain';

CREATE INDEX IF NOT EXISTS idx_fact_crypto_price_metrics_date_time
    ON fact_crypto_price_metrics (date_key, time_key);

CREATE INDEX IF NOT EXISTS idx_fact_crypto_price_metrics_symbol
    ON fact_crypto_price_metrics (crypto_key);

-- ---------------------------------------------------------------------------
-- 4. Dimension upserts (DML)
-- ---------------------------------------------------------------------------

-- dim_cryptocurrency --------------------------------------------------------
WITH src AS (
    SELECT
        c.crypto_id,
        c.symbol,
        c.name,
        c.image_url,
        MIN(ps.snapshot_time) AS first_snapshot_at,
        MAX(c.updated_at) AS last_source_update_at
    FROM cryptocurrencies c
    LEFT JOIN price_snapshots ps ON ps.crypto_id = c.crypto_id
    GROUP BY c.crypto_id, c.symbol, c.name, c.image_url
)
INSERT INTO dim_cryptocurrency (
    crypto_id,
    symbol,
    name,
    image_url,
    first_snapshot_at,
    last_source_update_at,
    updated_at
)
SELECT
    s.crypto_id,
    s.symbol,
    s.name,
    s.image_url,
    s.first_snapshot_at,
    COALESCE(s.last_source_update_at, CURRENT_TIMESTAMP) AS last_source_update_at,
    CURRENT_TIMESTAMP AS updated_at
FROM src s
ON CONFLICT (crypto_id) DO UPDATE
SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name,
    image_url = EXCLUDED.image_url,
    last_source_update_at = EXCLUDED.last_source_update_at,
    updated_at = CURRENT_TIMESTAMP,
    first_snapshot_at = LEAST(dim_cryptocurrency.first_snapshot_at, EXCLUDED.first_snapshot_at);

-- dim_source ----------------------------------------------------------------
WITH src AS (
    SELECT
        ib.source AS source_name,
        MIN(ib.ingested_at) AS first_ingested_at,
        MAX(ib.ingested_at) AS last_ingested_at
    FROM ingestion_batches ib
    GROUP BY ib.source
)
INSERT INTO dim_source (
    source_name,
    first_ingested_at,
    last_ingested_at,
    updated_at
)
SELECT
    s.source_name,
    s.first_ingested_at,
    s.last_ingested_at,
    CURRENT_TIMESTAMP AS updated_at
FROM src s
ON CONFLICT (source_name) DO UPDATE
SET
    first_ingested_at = LEAST(dim_source.first_ingested_at, EXCLUDED.first_ingested_at),
    last_ingested_at = GREATEST(dim_source.last_ingested_at, EXCLUDED.last_ingested_at),
    updated_at = CURRENT_TIMESTAMP;

-- dim_date ------------------------------------------------------------------
WITH distinct_dates AS (
    SELECT DISTINCT DATE(ps.snapshot_time) AS full_date
    FROM price_snapshots ps
)
INSERT INTO dim_date (
    date_key,
    full_date,
    day_of_month,
    day_name,
    iso_week,
    month_of_year,
    month_name,
    quarter_of_year,
    calendar_year,
    is_weekend
)
SELECT
    (EXTRACT(YEAR FROM d.full_date) * 10000
        + EXTRACT(MONTH FROM d.full_date) * 100
        + EXTRACT(DAY FROM d.full_date))::INTEGER AS date_key,
    d.full_date,
    EXTRACT(DAY FROM d.full_date)::SMALLINT AS day_of_month,
    TO_CHAR(d.full_date, 'Dy') AS day_name,
    EXTRACT(ISOWEEK FROM d.full_date)::SMALLINT AS iso_week,
    EXTRACT(MONTH FROM d.full_date)::SMALLINT AS month_of_year,
    TO_CHAR(d.full_date, 'Mon') AS month_name,
    EXTRACT(QUARTER FROM d.full_date)::SMALLINT AS quarter_of_year,
    EXTRACT(YEAR FROM d.full_date)::INTEGER AS calendar_year,
    (EXTRACT(ISODOW FROM d.full_date) IN (6, 7)) AS is_weekend
FROM distinct_dates d
ON CONFLICT (date_key) DO UPDATE
SET
    day_name = EXCLUDED.day_name,
    iso_week = EXCLUDED.iso_week,
    month_of_year = EXCLUDED.month_of_year,
    month_name = EXCLUDED.month_name,
    quarter_of_year = EXCLUDED.quarter_of_year,
    calendar_year = EXCLUDED.calendar_year,
    is_weekend = EXCLUDED.is_weekend;

-- dim_time ------------------------------------------------------------------
WITH distinct_times AS (
    SELECT DISTINCT DATE_TRUNC('minute', ps.snapshot_time)::TIME AS full_time
    FROM price_snapshots ps
)
INSERT INTO dim_time (
    time_key,
    full_time,
    hour_24,
    minute_of_hour,
    second_of_minute
)
SELECT
    ((EXTRACT(HOUR FROM t.full_time) * 100) + EXTRACT(MINUTE FROM t.full_time))::INTEGER AS time_key,
    t.full_time,
    EXTRACT(HOUR FROM t.full_time)::SMALLINT AS hour_24,
    EXTRACT(MINUTE FROM t.full_time)::SMALLINT AS minute_of_hour,
    EXTRACT(SECOND FROM t.full_time)::SMALLINT AS second_of_minute
FROM distinct_times t
ON CONFLICT (time_key) DO UPDATE
SET
    full_time = EXCLUDED.full_time,
    hour_24 = EXCLUDED.hour_24,
    minute_of_hour = EXCLUDED.minute_of_hour,
    second_of_minute = EXCLUDED.second_of_minute;

-- ---------------------------------------------------------------------------
-- 5. Fact table upsert (DML)
-- ---------------------------------------------------------------------------

WITH snapshot_metrics AS (
    SELECT
        ps.snapshot_id,
        ps.crypto_id,
        ps.batch_id,
        ps.snapshot_time,
        ps.last_updated,
        ps.current_price,
        ps.high_24h,
        ps.low_24h,
        ps.price_change_24h,
        ps.price_change_pct_24h,
        ps.ath,
        ps.ath_change_pct,
        ps.atl,
        ps.atl_change_pct,
        mm.market_cap,
        mm.market_cap_rank,
        mm.fully_diluted_valuation,
        mm.total_volume,
        mm.market_cap_change_24h,
        mm.market_cap_change_pct_24h,
        mm.circulating_supply,
        mm.total_supply,
        mm.max_supply,
        mm.roi_times,
        mm.roi_percentage,
        ib.source,
        DATE(ps.snapshot_time) AS full_date,
        DATE_TRUNC('minute', ps.snapshot_time)::TIME AS full_time
    FROM price_snapshots ps
    INNER JOIN market_metrics mm ON mm.snapshot_id = ps.snapshot_id
    INNER JOIN ingestion_batches ib ON ib.batch_id = ps.batch_id
)
INSERT INTO fact_crypto_price_metrics (
    crypto_key,
    date_key,
    time_key,
    source_key,
    batch_id,
    snapshot_timestamp,
    last_updated_at,
    current_price,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_pct_24h,
    ath,
    ath_change_pct,
    atl,
    atl_change_pct,
    market_cap,
    market_cap_rank,
    fully_diluted_valuation,
    total_volume,
    market_cap_change_24h,
    market_cap_change_pct_24h,
    circulating_supply,
    total_supply,
    max_supply,
    roi_times,
    roi_percentage,
    load_timestamp
)
SELECT
    dc.crypto_key,
    dd.date_key,
    dt.time_key,
    ds.source_key,
    sm.batch_id,
    sm.snapshot_time,
    sm.last_updated,
    sm.current_price,
    sm.high_24h,
    sm.low_24h,
    sm.price_change_24h,
    sm.price_change_pct_24h,
    sm.ath,
    sm.ath_change_pct,
    sm.atl,
    sm.atl_change_pct,
    sm.market_cap,
    sm.market_cap_rank,
    sm.fully_diluted_valuation,
    sm.total_volume,
    sm.market_cap_change_24h,
    sm.market_cap_change_pct_24h,
    sm.circulating_supply,
    sm.total_supply,
    sm.max_supply,
    sm.roi_times,
    sm.roi_percentage,
    CURRENT_TIMESTAMP AS load_timestamp
FROM snapshot_metrics sm
INNER JOIN dim_cryptocurrency dc ON dc.crypto_id = sm.crypto_id
INNER JOIN dim_date dd ON dd.full_date = sm.full_date
INNER JOIN dim_time dt ON dt.full_time = sm.full_time
INNER JOIN dim_source ds ON ds.source_name = sm.source
ON CONFLICT (crypto_key, snapshot_timestamp) DO UPDATE
SET
    last_updated_at = EXCLUDED.last_updated_at,
    current_price = EXCLUDED.current_price,
    high_24h = EXCLUDED.high_24h,
    low_24h = EXCLUDED.low_24h,
    price_change_24h = EXCLUDED.price_change_24h,
    price_change_pct_24h = EXCLUDED.price_change_pct_24h,
    ath = EXCLUDED.ath,
    ath_change_pct = EXCLUDED.ath_change_pct,
    atl = EXCLUDED.atl,
    atl_change_pct = EXCLUDED.atl_change_pct,
    market_cap = EXCLUDED.market_cap,
    market_cap_rank = EXCLUDED.market_cap_rank,
    fully_diluted_valuation = EXCLUDED.fully_diluted_valuation,
    total_volume = EXCLUDED.total_volume,
    market_cap_change_24h = EXCLUDED.market_cap_change_24h,
    market_cap_change_pct_24h = EXCLUDED.market_cap_change_pct_24h,
    circulating_supply = EXCLUDED.circulating_supply,
    total_supply = EXCLUDED.total_supply,
    max_supply = EXCLUDED.max_supply,
    roi_times = EXCLUDED.roi_times,
    roi_percentage = EXCLUDED.roi_percentage,
    load_timestamp = CURRENT_TIMESTAMP;

-- Optional: refresh table statistics for the new star schema
ANALYZE dim_cryptocurrency;
ANALYZE dim_source;
ANALYZE dim_date;
ANALYZE dim_time;
ANALYZE fact_crypto_price_metrics;
