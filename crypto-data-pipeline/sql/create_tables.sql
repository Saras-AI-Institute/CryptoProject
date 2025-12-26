-- ============================================================================
-- Crypto Data Pipeline - Database Schema
-- ============================================================================
-- Description: Creates normalized (3NF) schema for cryptocurrency price data
-- Database: PostgreSQL
-- Version: 1.0
-- ============================================================================

-- Drop existing tables (in reverse order of dependencies)
DROP TABLE IF EXISTS market_metrics CASCADE;
DROP TABLE IF EXISTS price_snapshots CASCADE;
DROP TABLE IF EXISTS cryptocurrencies CASCADE;
DROP TABLE IF EXISTS ingestion_batches CASCADE;

-- ============================================================================
-- TABLE: ingestion_batches
-- Purpose: Track data ingestion events for audit and lineage
-- ============================================================================
CREATE TABLE ingestion_batches (
    batch_id SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP NOT NULL,
    source VARCHAR(100) NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_status CHECK (status IN ('pending', 'completed', 'failed'))
);

-- Index for querying recent batches
CREATE INDEX idx_ingestion_batches_ingested_at ON ingestion_batches(ingested_at DESC);
CREATE INDEX idx_ingestion_batches_status ON ingestion_batches(status);

COMMENT ON TABLE ingestion_batches IS 'Tracks each data ingestion batch for audit trail';
COMMENT ON COLUMN ingestion_batches.batch_id IS 'Unique identifier for each ingestion batch';
COMMENT ON COLUMN ingestion_batches.ingested_at IS 'Timestamp when data was ingested from source';
COMMENT ON COLUMN ingestion_batches.source IS 'Data source name (e.g., CoinGecko)';
COMMENT ON COLUMN ingestion_batches.record_count IS 'Number of crypto records in this batch';
COMMENT ON COLUMN ingestion_batches.status IS 'Batch processing status: pending, completed, failed';

-- ============================================================================
-- TABLE: cryptocurrencies
-- Purpose: Master table for cryptocurrency static/slowly changing data
-- ============================================================================
CREATE TABLE cryptocurrencies (
    crypto_id VARCHAR(50) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    image_url VARCHAR(500),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_cryptocurrencies_symbol UNIQUE (symbol)
);

-- Indexes for common queries
CREATE INDEX idx_cryptocurrencies_symbol ON cryptocurrencies(symbol);
CREATE INDEX idx_cryptocurrencies_name ON cryptocurrencies(name);

COMMENT ON TABLE cryptocurrencies IS 'Master table containing cryptocurrency reference data';
COMMENT ON COLUMN cryptocurrencies.crypto_id IS 'Unique identifier from data source (e.g., bitcoin)';
COMMENT ON COLUMN cryptocurrencies.symbol IS 'Trading symbol (e.g., btc, eth)';
COMMENT ON COLUMN cryptocurrencies.name IS 'Full cryptocurrency name';
COMMENT ON COLUMN cryptocurrencies.image_url IS 'URL to cryptocurrency logo/image';
COMMENT ON COLUMN cryptocurrencies.updated_at IS 'Last time this record was updated';

-- ============================================================================
-- TABLE: price_snapshots
-- Purpose: Time-series price data and historical metrics
-- ============================================================================
CREATE TABLE price_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    crypto_id VARCHAR(50) NOT NULL,
    batch_id INTEGER NOT NULL,
    current_price DECIMAL(20, 8) NOT NULL,
    high_24h DECIMAL(20, 8),
    low_24h DECIMAL(20, 8),
    price_change_24h DECIMAL(20, 8),
    price_change_pct_24h DECIMAL(10, 5),
    ath DECIMAL(20, 8),
    ath_change_pct DECIMAL(10, 5),
    ath_date TIMESTAMP,
    atl DECIMAL(20, 8),
    atl_change_pct DECIMAL(10, 5),
    atl_date TIMESTAMP,
    last_updated TIMESTAMP NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    CONSTRAINT fk_price_snapshots_crypto 
        FOREIGN KEY (crypto_id) 
        REFERENCES cryptocurrencies(crypto_id) 
        ON DELETE CASCADE,
    
    CONSTRAINT fk_price_snapshots_batch 
        FOREIGN KEY (batch_id) 
        REFERENCES ingestion_batches(batch_id) 
        ON DELETE CASCADE,
    
    -- Unique constraint for deduplication
    CONSTRAINT uq_price_snapshots_crypto_time 
        UNIQUE (crypto_id, snapshot_time),
    
    -- Data validation constraints
    CONSTRAINT chk_price_positive CHECK (current_price > 0),
    CONSTRAINT chk_high_low CHECK (high_24h IS NULL OR low_24h IS NULL OR high_24h >= low_24h)
);

-- Indexes for time-series queries and performance
CREATE INDEX idx_price_snapshots_crypto_time ON price_snapshots(crypto_id, snapshot_time DESC);
CREATE INDEX idx_price_snapshots_snapshot_time ON price_snapshots(snapshot_time DESC);
CREATE INDEX idx_price_snapshots_batch ON price_snapshots(batch_id);
CREATE INDEX idx_price_snapshots_last_updated ON price_snapshots(last_updated DESC);

COMMENT ON TABLE price_snapshots IS 'Time-series price data and historical performance metrics';
COMMENT ON COLUMN price_snapshots.snapshot_id IS 'Unique identifier for each price snapshot';
COMMENT ON COLUMN price_snapshots.crypto_id IS 'Reference to cryptocurrency';
COMMENT ON COLUMN price_snapshots.batch_id IS 'Reference to ingestion batch';
COMMENT ON COLUMN price_snapshots.current_price IS 'Current price in USD';
COMMENT ON COLUMN price_snapshots.high_24h IS '24-hour high price';
COMMENT ON COLUMN price_snapshots.low_24h IS '24-hour low price';
COMMENT ON COLUMN price_snapshots.price_change_24h IS 'Absolute price change in last 24 hours';
COMMENT ON COLUMN price_snapshots.price_change_pct_24h IS 'Percentage price change in last 24 hours';
COMMENT ON COLUMN price_snapshots.ath IS 'All-time high price';
COMMENT ON COLUMN price_snapshots.ath_change_pct IS 'Percentage change from all-time high';
COMMENT ON COLUMN price_snapshots.ath_date IS 'Date when all-time high was reached';
COMMENT ON COLUMN price_snapshots.atl IS 'All-time low price';
COMMENT ON COLUMN price_snapshots.atl_change_pct IS 'Percentage change from all-time low';
COMMENT ON COLUMN price_snapshots.atl_date IS 'Date when all-time low was reached';
COMMENT ON COLUMN price_snapshots.last_updated IS 'Source timestamp of the data';
COMMENT ON COLUMN price_snapshots.snapshot_time IS 'Normalized snapshot time (rounded for deduplication)';

-- ============================================================================
-- TABLE: market_metrics
-- Purpose: Market capitalization and supply metrics
-- ============================================================================
CREATE TABLE market_metrics (
    metric_id SERIAL PRIMARY KEY,
    snapshot_id INTEGER NOT NULL,
    market_cap BIGINT,
    market_cap_rank INTEGER,
    fully_diluted_valuation BIGINT,
    total_volume BIGINT,
    market_cap_change_24h BIGINT,
    market_cap_change_pct_24h DECIMAL(10, 5),
    circulating_supply DECIMAL(30, 8),
    total_supply DECIMAL(30, 8),
    max_supply DECIMAL(30, 8),
    roi_times DECIMAL(20, 8),
    roi_currency VARCHAR(10),
    roi_percentage DECIMAL(20, 8),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint (1:1 relationship with price_snapshots)
    CONSTRAINT fk_market_metrics_snapshot 
        FOREIGN KEY (snapshot_id) 
        REFERENCES price_snapshots(snapshot_id) 
        ON DELETE CASCADE,
    
    -- Ensure 1:1 relationship
    CONSTRAINT uq_market_metrics_snapshot UNIQUE (snapshot_id),
    
    -- Data validation constraints
    CONSTRAINT chk_market_cap_positive CHECK (market_cap IS NULL OR market_cap >= 0),
    CONSTRAINT chk_rank_positive CHECK (market_cap_rank IS NULL OR market_cap_rank > 0),
    CONSTRAINT chk_supply_order CHECK (
        max_supply IS NULL OR 
        total_supply IS NULL OR 
        circulating_supply IS NULL OR 
        max_supply >= total_supply
    )
);

-- Index for join optimization
CREATE INDEX idx_market_metrics_snapshot ON market_metrics(snapshot_id);

COMMENT ON TABLE market_metrics IS 'Market capitalization, trading volume, and supply metrics';
COMMENT ON COLUMN market_metrics.metric_id IS 'Unique identifier for market metrics record';
COMMENT ON COLUMN market_metrics.snapshot_id IS 'Reference to price snapshot (1:1 relationship)';
COMMENT ON COLUMN market_metrics.market_cap IS 'Market capitalization in USD';
COMMENT ON COLUMN market_metrics.market_cap_rank IS 'Ranking by market capitalization';
COMMENT ON COLUMN market_metrics.fully_diluted_valuation IS 'Valuation if max supply is reached';
COMMENT ON COLUMN market_metrics.total_volume IS '24-hour trading volume in USD';
COMMENT ON COLUMN market_metrics.market_cap_change_24h IS 'Absolute market cap change in 24 hours';
COMMENT ON COLUMN market_metrics.market_cap_change_pct_24h IS 'Percentage market cap change in 24 hours';
COMMENT ON COLUMN market_metrics.circulating_supply IS 'Number of coins in circulation';
COMMENT ON COLUMN market_metrics.total_supply IS 'Total number of coins issued';
COMMENT ON COLUMN market_metrics.max_supply IS 'Maximum possible supply';
COMMENT ON COLUMN market_metrics.roi_times IS 'Return on investment multiplier';
COMMENT ON COLUMN market_metrics.roi_currency IS 'Base currency for ROI calculation';
COMMENT ON COLUMN market_metrics.roi_percentage IS 'Return on investment percentage';

-- ============================================================================
-- VIEWS: Convenient data access
-- ============================================================================

-- View: Latest prices for all cryptocurrencies
CREATE OR REPLACE VIEW v_latest_prices AS
SELECT 
    c.crypto_id,
    c.symbol,
    c.name,
    ps.current_price,
    ps.price_change_pct_24h,
    ps.high_24h,
    ps.low_24h,
    mm.market_cap,
    mm.market_cap_rank,
    mm.total_volume,
    ps.snapshot_time,
    ps.last_updated
FROM cryptocurrencies c
INNER JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
INNER JOIN market_metrics mm ON ps.snapshot_id = mm.snapshot_id
WHERE ps.snapshot_time = (
    SELECT MAX(snapshot_time)
    FROM price_snapshots ps2
    WHERE ps2.crypto_id = ps.crypto_id
)
ORDER BY mm.market_cap_rank;

COMMENT ON VIEW v_latest_prices IS 'Shows the most recent price snapshot for each cryptocurrency';

-- View: Price history with market metrics
CREATE OR REPLACE VIEW v_price_history AS
SELECT 
    c.crypto_id,
    c.symbol,
    c.name,
    ps.snapshot_time,
    ps.current_price,
    ps.price_change_24h,
    ps.price_change_pct_24h,
    ps.high_24h,
    ps.low_24h,
    ps.ath,
    ps.ath_change_pct,
    ps.atl,
    ps.atl_change_pct,
    mm.market_cap,
    mm.market_cap_rank,
    mm.total_volume,
    mm.circulating_supply,
    ib.source,
    ib.batch_id
FROM cryptocurrencies c
INNER JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
INNER JOIN market_metrics mm ON ps.snapshot_id = mm.snapshot_id
INNER JOIN ingestion_batches ib ON ps.batch_id = ib.batch_id
ORDER BY ps.snapshot_time DESC, mm.market_cap_rank;

COMMENT ON VIEW v_price_history IS 'Complete price history with all metrics for analysis';

-- ============================================================================
-- FUNCTIONS: Helper utilities
-- ============================================================================

-- Function: Round timestamp to nearest 5 minutes for snapshot deduplication
CREATE OR REPLACE FUNCTION round_to_snapshot_interval(ts TIMESTAMP, interval_minutes INTEGER DEFAULT 5)
RETURNS TIMESTAMP AS $$
BEGIN
    RETURN date_trunc('hour', ts) + 
           (INTERVAL '1 minute' * interval_minutes * 
            ROUND(EXTRACT(MINUTE FROM ts)::NUMERIC / interval_minutes));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION round_to_snapshot_interval IS 'Rounds timestamp to nearest interval for deduplication';

-- Function: Update cryptocurrency updated_at timestamp
CREATE OR REPLACE FUNCTION update_cryptocurrency_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: Auto-update cryptocurrency timestamp
CREATE TRIGGER trg_update_cryptocurrency_timestamp
BEFORE UPDATE ON cryptocurrencies
FOR EACH ROW
EXECUTE FUNCTION update_cryptocurrency_timestamp();

-- ============================================================================
-- GRANTS: Set appropriate permissions (modify as needed)
-- ============================================================================

-- Example: Grant read access to analytics role
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_role;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO analytics_role;

-- Example: Grant write access to etl role
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_role;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO etl_role;

-- ============================================================================
-- COMPLETION
-- ============================================================================

-- Display table information
SELECT 
    'Tables created successfully!' AS status,
    COUNT(*) AS table_count
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_type = 'BASE TABLE'
  AND table_name IN ('ingestion_batches', 'cryptocurrencies', 'price_snapshots', 'market_metrics');
