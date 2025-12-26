-- ============================================================================
-- Crypto Data Pipeline - Common SQL Queries
-- ============================================================================
-- Quick reference for frequently used queries
-- ============================================================================

-- ============================================================================
-- SECTION 1: Data Retrieval
-- ============================================================================

-- Get latest price for all cryptocurrencies
SELECT * FROM v_latest_prices
ORDER BY market_cap_rank;

-- Get latest price for specific cryptocurrency
SELECT 
    name,
    symbol,
    current_price,
    price_change_pct_24h,
    market_cap,
    snapshot_time
FROM v_latest_prices
WHERE symbol = 'btc';

-- Get price history for last 24 hours
SELECT 
    snapshot_time,
    current_price,
    high_24h,
    low_24h,
    total_volume
FROM v_price_history
WHERE symbol = 'btc'
  AND snapshot_time >= NOW() - INTERVAL '24 hours'
ORDER BY snapshot_time DESC;

-- Get all snapshots for a cryptocurrency
SELECT 
    ps.snapshot_time,
    ps.current_price,
    ps.price_change_pct_24h,
    mm.market_cap,
    mm.total_volume
FROM price_snapshots ps
JOIN market_metrics mm ON ps.snapshot_id = mm.snapshot_id
WHERE ps.crypto_id = 'bitcoin'
ORDER BY ps.snapshot_time DESC
LIMIT 100;

-- ============================================================================
-- SECTION 2: Time-Series Analysis
-- ============================================================================

-- Hourly average prices for last week
SELECT 
    crypto_id,
    date_trunc('hour', snapshot_time) AS hour,
    AVG(current_price) AS avg_price,
    MIN(current_price) AS min_price,
    MAX(current_price) AS max_price,
    COUNT(*) AS snapshot_count
FROM price_snapshots
WHERE crypto_id = 'bitcoin'
  AND snapshot_time >= NOW() - INTERVAL '7 days'
GROUP BY crypto_id, date_trunc('hour', snapshot_time)
ORDER BY hour DESC;

-- Daily price summary
SELECT 
    crypto_id,
    DATE(snapshot_time) AS day,
    MIN(current_price) AS day_low,
    MAX(current_price) AS day_high,
    AVG(current_price) AS day_avg,
    (MAX(current_price) - MIN(current_price)) / MIN(current_price) * 100 AS volatility_pct,
    COUNT(*) AS snapshots
FROM price_snapshots
WHERE crypto_id = 'ethereum'
  AND snapshot_time >= NOW() - INTERVAL '30 days'
GROUP BY crypto_id, DATE(snapshot_time)
ORDER BY day DESC;

-- Price changes over time windows
SELECT 
    c.symbol,
    ps_current.current_price AS current_price,
    ps_1h.current_price AS price_1h_ago,
    ps_24h.current_price AS price_24h_ago,
    ps_7d.current_price AS price_7d_ago,
    ROUND(((ps_current.current_price - ps_1h.current_price) / ps_1h.current_price * 100)::NUMERIC, 2) AS change_1h_pct,
    ROUND(((ps_current.current_price - ps_24h.current_price) / ps_24h.current_price * 100)::NUMERIC, 2) AS change_24h_pct,
    ROUND(((ps_current.current_price - ps_7d.current_price) / ps_7d.current_price * 100)::NUMERIC, 2) AS change_7d_pct
FROM cryptocurrencies c
CROSS JOIN LATERAL (
    SELECT current_price, snapshot_time
    FROM price_snapshots
    WHERE crypto_id = c.crypto_id
    ORDER BY snapshot_time DESC
    LIMIT 1
) ps_current
LEFT JOIN LATERAL (
    SELECT current_price
    FROM price_snapshots
    WHERE crypto_id = c.crypto_id
      AND snapshot_time <= ps_current.snapshot_time - INTERVAL '1 hour'
    ORDER BY snapshot_time DESC
    LIMIT 1
) ps_1h ON TRUE
LEFT JOIN LATERAL (
    SELECT current_price
    FROM price_snapshots
    WHERE crypto_id = c.crypto_id
      AND snapshot_time <= ps_current.snapshot_time - INTERVAL '24 hours'
    ORDER BY snapshot_time DESC
    LIMIT 1
) ps_24h ON TRUE
LEFT JOIN LATERAL (
    SELECT current_price
    FROM price_snapshots
    WHERE crypto_id = c.crypto_id
      AND snapshot_time <= ps_current.snapshot_time - INTERVAL '7 days'
    ORDER BY snapshot_time DESC
    LIMIT 1
) ps_7d ON TRUE
ORDER BY c.symbol;

-- ============================================================================
-- SECTION 3: Market Analysis
-- ============================================================================

-- Top cryptocurrencies by market cap
SELECT 
    c.name,
    c.symbol,
    ps.current_price,
    mm.market_cap,
    mm.market_cap_rank,
    mm.total_volume,
    mm.circulating_supply
FROM cryptocurrencies c
JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
JOIN market_metrics mm ON ps.snapshot_id = mm.snapshot_id
WHERE ps.snapshot_time = (
    SELECT MAX(snapshot_time)
    FROM price_snapshots
)
ORDER BY mm.market_cap_rank
LIMIT 10;

-- Market dominance analysis
WITH total_market AS (
    SELECT SUM(mm.market_cap) AS total_cap
    FROM market_metrics mm
    JOIN price_snapshots ps ON mm.snapshot_id = ps.snapshot_id
    WHERE ps.snapshot_time = (
        SELECT MAX(snapshot_time)
        FROM price_snapshots
    )
)
SELECT 
    c.symbol,
    c.name,
    mm.market_cap,
    ROUND((mm.market_cap::NUMERIC / tm.total_cap * 100), 2) AS dominance_pct
FROM cryptocurrencies c
JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
JOIN market_metrics mm ON ps.snapshot_id = mm.snapshot_id
CROSS JOIN total_market tm
WHERE ps.snapshot_time = (
    SELECT MAX(snapshot_time)
    FROM price_snapshots
)
ORDER BY dominance_pct DESC;

-- Volume to market cap ratio (liquidity indicator)
SELECT 
    c.symbol,
    c.name,
    mm.total_volume,
    mm.market_cap,
    ROUND((mm.total_volume::NUMERIC / mm.market_cap * 100), 2) AS volume_mcap_ratio
FROM cryptocurrencies c
JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
JOIN market_metrics mm ON ps.snapshot_id = mm.snapshot_id
WHERE ps.snapshot_time = (
    SELECT MAX(snapshot_time)
    FROM price_snapshots
)
ORDER BY volume_mcap_ratio DESC;

-- ============================================================================
-- SECTION 4: Historical Performance
-- ============================================================================

-- All-time high/low comparison
SELECT 
    c.symbol,
    c.name,
    ps.current_price,
    ps.ath,
    ps.ath_date,
    ps.ath_change_pct,
    ps.atl,
    ps.atl_date,
    ps.atl_change_pct,
    ROUND(((ps.ath - ps.atl) / ps.atl * 100)::NUMERIC, 2) AS ath_atl_gain_pct
FROM cryptocurrencies c
JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
WHERE ps.snapshot_time = (
    SELECT MAX(snapshot_time)
    FROM price_snapshots ps2
    WHERE ps2.crypto_id = ps.crypto_id
)
ORDER BY ath_atl_gain_pct DESC;

-- Distance from all-time high
SELECT 
    c.symbol,
    ps.current_price,
    ps.ath,
    ps.ath_date,
    ps.ath_change_pct,
    CASE 
        WHEN ps.ath_change_pct > -10 THEN 'Near ATH'
        WHEN ps.ath_change_pct > -30 THEN 'Moderate Correction'
        WHEN ps.ath_change_pct > -50 THEN 'Significant Correction'
        ELSE 'Deep Correction'
    END AS correction_level
FROM cryptocurrencies c
JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
WHERE ps.snapshot_time = (
    SELECT MAX(snapshot_time)
    FROM price_snapshots ps2
    WHERE ps2.crypto_id = ps.crypto_id
)
ORDER BY ps.ath_change_pct DESC;

-- ============================================================================
-- SECTION 5: Data Quality & Monitoring
-- ============================================================================

-- Ingestion batch summary
SELECT 
    batch_id,
    ingested_at,
    source,
    record_count,
    status,
    created_at
FROM ingestion_batches
ORDER BY ingested_at DESC
LIMIT 20;

-- Deduplication statistics
SELECT 
    ib.batch_id,
    ib.ingested_at,
    ib.record_count AS records_ingested,
    COUNT(ps.snapshot_id) AS unique_snapshots,
    ib.record_count - COUNT(ps.snapshot_id) AS duplicates_skipped,
    ROUND(((ib.record_count - COUNT(ps.snapshot_id))::NUMERIC / ib.record_count * 100), 2) AS duplicate_pct
FROM ingestion_batches ib
LEFT JOIN price_snapshots ps ON ib.batch_id = ps.batch_id
GROUP BY ib.batch_id, ib.ingested_at, ib.record_count
ORDER BY ib.ingested_at DESC
LIMIT 20;

-- Daily ingestion summary
SELECT 
    DATE(ingested_at) AS ingestion_date,
    COUNT(DISTINCT batch_id) AS total_batches,
    SUM(record_count) AS total_records,
    COUNT(DISTINCT ps.snapshot_id) AS unique_snapshots,
    SUM(record_count) - COUNT(DISTINCT ps.snapshot_id) AS duplicates_prevented
FROM ingestion_batches ib
LEFT JOIN price_snapshots ps ON ib.batch_id = ps.batch_id
GROUP BY DATE(ingested_at)
ORDER BY ingestion_date DESC;

-- Data freshness check
SELECT 
    c.symbol,
    MAX(ps.snapshot_time) AS latest_snapshot,
    MAX(ps.last_updated) AS latest_source_update,
    NOW() - MAX(ps.snapshot_time) AS data_age
FROM cryptocurrencies c
JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
GROUP BY c.symbol
ORDER BY data_age DESC;

-- Snapshot count by cryptocurrency
SELECT 
    c.symbol,
    COUNT(ps.snapshot_id) AS snapshot_count,
    MIN(ps.snapshot_time) AS first_snapshot,
    MAX(ps.snapshot_time) AS last_snapshot,
    MAX(ps.snapshot_time) - MIN(ps.snapshot_time) AS time_span
FROM cryptocurrencies c
JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
GROUP BY c.symbol
ORDER BY snapshot_count DESC;

-- Identify missing snapshots (gaps in time series)
WITH expected_snapshots AS (
    SELECT 
        crypto_id,
        generate_series(
            MIN(snapshot_time),
            MAX(snapshot_time),
            INTERVAL '5 minutes'
        ) AS expected_time
    FROM price_snapshots
    GROUP BY crypto_id
)
SELECT 
    es.crypto_id,
    es.expected_time,
    'MISSING' AS status
FROM expected_snapshots es
LEFT JOIN price_snapshots ps 
    ON es.crypto_id = ps.crypto_id 
    AND es.expected_time = ps.snapshot_time
WHERE ps.snapshot_id IS NULL
ORDER BY es.crypto_id, es.expected_time
LIMIT 100;

-- ============================================================================
-- SECTION 6: Data Cleanup & Maintenance
-- ============================================================================

-- Delete old ingestion batches (keep last 90 days)
-- CAUTION: This will cascade delete associated price snapshots
/*
DELETE FROM ingestion_batches
WHERE ingested_at < NOW() - INTERVAL '90 days';
*/

-- Archive old snapshots to separate table
/*
CREATE TABLE price_snapshots_archive (LIKE price_snapshots INCLUDING ALL);

INSERT INTO price_snapshots_archive
SELECT * FROM price_snapshots
WHERE snapshot_time < NOW() - INTERVAL '1 year';

DELETE FROM price_snapshots
WHERE snapshot_time < NOW() - INTERVAL '1 year';
*/

-- Vacuum and analyze tables for performance
/*
VACUUM ANALYZE ingestion_batches;
VACUUM ANALYZE cryptocurrencies;
VACUUM ANALYZE price_snapshots;
VACUUM ANALYZE market_metrics;
*/

-- ============================================================================
-- SECTION 7: Advanced Analytics
-- ============================================================================

-- Moving average (7-day)
SELECT 
    crypto_id,
    snapshot_time,
    current_price,
    AVG(current_price) OVER (
        PARTITION BY crypto_id 
        ORDER BY snapshot_time 
        ROWS BETWEEN 2015 PRECEDING AND CURRENT ROW
    ) AS ma_7day
FROM price_snapshots
WHERE crypto_id = 'bitcoin'
  AND snapshot_time >= NOW() - INTERVAL '30 days'
ORDER BY snapshot_time DESC;

-- Volatility calculation (standard deviation of returns)
WITH price_changes AS (
    SELECT 
        crypto_id,
        snapshot_time,
        current_price,
        LAG(current_price) OVER (PARTITION BY crypto_id ORDER BY snapshot_time) AS prev_price,
        (current_price - LAG(current_price) OVER (PARTITION BY crypto_id ORDER BY snapshot_time)) 
            / LAG(current_price) OVER (PARTITION BY crypto_id ORDER BY snapshot_time) * 100 AS price_change_pct
    FROM price_snapshots
    WHERE snapshot_time >= NOW() - INTERVAL '7 days'
)
SELECT 
    c.symbol,
    COUNT(pc.snapshot_time) AS sample_size,
    ROUND(AVG(pc.price_change_pct)::NUMERIC, 4) AS avg_return_pct,
    ROUND(STDDEV(pc.price_change_pct)::NUMERIC, 4) AS volatility_pct
FROM cryptocurrencies c
JOIN price_changes pc ON c.crypto_id = pc.crypto_id
WHERE pc.price_change_pct IS NOT NULL
GROUP BY c.symbol
ORDER BY volatility_pct DESC;

-- Correlation between cryptocurrencies (requires crosstab extension)
/*
CREATE EXTENSION IF NOT EXISTS tablefunc;

WITH btc_prices AS (
    SELECT snapshot_time, current_price AS btc_price
    FROM price_snapshots
    WHERE crypto_id = 'bitcoin'
      AND snapshot_time >= NOW() - INTERVAL '30 days'
),
eth_prices AS (
    SELECT snapshot_time, current_price AS eth_price
    FROM price_snapshots
    WHERE crypto_id = 'ethereum'
      AND snapshot_time >= NOW() - INTERVAL '30 days'
)
SELECT 
    CORR(b.btc_price, e.eth_price) AS btc_eth_correlation
FROM btc_prices b
JOIN eth_prices e ON b.snapshot_time = e.snapshot_time;
*/

-- ============================================================================
-- End of Common Queries
-- ============================================================================
