# Deduplication Strategy for Crypto Data Pipeline

## Overview

This document describes the comprehensive deduplication strategy for handling recurring and updated cryptocurrency price events in the data pipeline. The strategy ensures data consistency, prevents duplicates, and maintains historical integrity while optimizing storage and query performance.

## Problem Statement

Cryptocurrency price data presents several deduplication challenges:

1. **High-Frequency Updates**: Prices can be ingested multiple times per minute
2. **Recurring Events**: The same crypto data is fetched repeatedly
3. **Near-Duplicate Data**: Multiple ingestions within short time windows may have minimal differences
4. **Master Data Updates**: Cryptocurrency metadata (name, symbol) rarely changes but must stay current
5. **Historical Preservation**: Need to track changes while avoiding redundant snapshots

## Deduplication Approaches by Entity

### 1. Cryptocurrencies Table (Master Data)

**Strategy**: UPSERT with timestamp tracking

**Implementation**:
```sql
INSERT INTO cryptocurrencies (crypto_id, symbol, name, image_url)
VALUES (%s, %s, %s, %s)
ON CONFLICT (crypto_id) 
DO UPDATE SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name,
    image_url = EXCLUDED.image_url,
    updated_at = CURRENT_TIMESTAMP
```

**Rationale**:
- Cryptocurrency master data changes infrequently (rebranding, logo updates)
- Primary key on `crypto_id` ensures uniqueness
- UPSERT pattern updates existing records without creating duplicates
- `updated_at` timestamp tracks when changes occurred
- No historical versions needed for master data (use Change Data Capture if required)

**Benefits**:
- Single source of truth for each cryptocurrency
- Automatically handles metadata updates
- Minimal storage overhead
- Simple queries without complex deduplication logic

---

### 2. Price Snapshots Table (Time-Series Data)

**Strategy**: Time-bucketed snapshots with unique constraint

**Implementation**:

#### Step 1: Round timestamps to snapshot intervals
```python
def round_to_snapshot_interval(timestamp, interval_minutes=5):
    """Round timestamp to nearest 5-minute interval"""
    return database_function('round_to_snapshot_interval', timestamp, interval_minutes)
```

```sql
-- Database function for consistent rounding
CREATE OR REPLACE FUNCTION round_to_snapshot_interval(
    ts TIMESTAMP, 
    interval_minutes INTEGER DEFAULT 5
)
RETURNS TIMESTAMP AS $$
BEGIN
    RETURN date_trunc('hour', ts) + 
           (INTERVAL '1 minute' * interval_minutes * 
            ROUND(EXTRACT(MINUTE FROM ts)::NUMERIC / interval_minutes));
END;
$$ LANGUAGE plpgsql IMMUTABLE;
```

#### Step 2: Insert with deduplication constraint
```sql
INSERT INTO price_snapshots (
    crypto_id, batch_id, current_price, ..., snapshot_time
)
VALUES (%s, %s, %s, ..., %s)
ON CONFLICT (crypto_id, snapshot_time) DO NOTHING
RETURNING snapshot_id;
```

**Rationale**:
- **Time Bucketing**: Groups near-duplicate snapshots into 5-minute intervals
  - Ingestion at 10:02:30 → snapshot_time = 10:00:00
  - Ingestion at 10:03:45 → snapshot_time = 10:05:00
  - Ingestion at 10:04:20 → snapshot_time = 10:05:00 (duplicate, skipped)
- **Unique Constraint**: `(crypto_id, snapshot_time)` prevents duplicates within the same interval
- **Silent Skipping**: `ON CONFLICT DO NOTHING` avoids errors for duplicates
- **Batch Tracking**: Even skipped records have their batch_id recorded for audit

**Configuration Options**:

| Interval | Use Case | Storage Impact | Data Freshness |
|----------|----------|----------------|----------------|
| 1 minute | Real-time trading | High | Excellent |
| 5 minutes | Near real-time analytics | Medium | Very Good |
| 15 minutes | Regular monitoring | Low | Good |
| 1 hour | Historical analysis | Very Low | Acceptable |

**Recommended**: 5 minutes for most use cases (balance between freshness and efficiency)

**Benefits**:
- Predictable snapshot times for queries
- Reduces storage by 50-90% compared to raw ingestion
- Simplifies time-series queries (no subquery deduplication needed)
- Preserves exact `last_updated` from source for audit

---

### 3. Market Metrics Table (Derived Time-Series)

**Strategy**: Inherited deduplication via 1:1 relationship

**Implementation**:
```sql
-- 1:1 relationship with price_snapshots
CONSTRAINT fk_market_metrics_snapshot 
    FOREIGN KEY (snapshot_id) 
    REFERENCES price_snapshots(snapshot_id) 
    ON DELETE CASCADE

-- Enforce 1:1 relationship
CONSTRAINT uq_market_metrics_snapshot UNIQUE (snapshot_id)
```

**Rationale**:
- Market metrics are always associated with a specific price snapshot
- Deduplication is handled by the parent `price_snapshots` table
- 1:1 relationship ensures no duplicate metrics for the same snapshot
- Cascade delete maintains referential integrity

**Benefits**:
- No additional deduplication logic required
- Guaranteed consistency with price snapshots
- Simplified data model

---

### 4. Ingestion Batches Table (Audit Trail)

**Strategy**: No deduplication - preserve all ingestion events

**Implementation**:
```sql
INSERT INTO ingestion_batches (ingested_at, source, record_count, status)
VALUES (%s, %s, %s, 'pending')
RETURNING batch_id;
```

**Rationale**:
- Every ingestion event is unique and valuable for audit
- Batch metadata includes:
  - When data was ingested (`ingested_at`)
  - Where it came from (`source`)
  - How many records were processed (`record_count`)
  - Processing outcome (`status`)
- Multiple batches can create the same snapshot (tracked via foreign key)

**Benefits**:
- Complete audit trail for compliance
- Debugging and troubleshooting capability
- Data lineage tracking
- Performance monitoring (batch processing times)

---

## Deduplication Decision Matrix

| Data Type | Deduplication Method | Key Constraint | Update Strategy |
|-----------|---------------------|----------------|-----------------|
| **Master Data** (cryptocurrencies) | UPSERT | crypto_id (PK) | UPDATE on conflict |
| **Time-Series** (price_snapshots) | Time bucketing | (crypto_id, snapshot_time) | INSERT or SKIP |
| **Metrics** (market_metrics) | Inherited | snapshot_id (1:1 FK) | INSERT only |
| **Audit** (ingestion_batches) | None | batch_id (PK) | INSERT only |

---

## Handling Edge Cases

### Case 1: Price Changes Within Snapshot Interval

**Scenario**: Bitcoin price changes from $87,000 to $87,500 within a 5-minute window

**Solution**: First-write-wins approach
```python
# First ingestion at 10:02:30 → snapshot_time = 10:00:00 → INSERTED ($87,000)
# Second ingestion at 10:04:00 → snapshot_time = 10:05:00 → INSERTED ($87,500)
# Third ingestion at 10:04:30 → snapshot_time = 10:05:00 → SKIPPED (duplicate)
```

**Alternative**: Last-write-wins with UPDATE
```sql
ON CONFLICT (crypto_id, snapshot_time) 
DO UPDATE SET
    current_price = EXCLUDED.current_price,
    -- Update other fields...
```

**Recommendation**: Use first-write-wins (DO NOTHING) for consistency and performance

---

### Case 2: Late-Arriving Data

**Scenario**: Data for 10:00 arrives after data for 10:05 due to retry or delay

**Solution**: Time-bucketing handles this naturally
```python
# Data arrives out of order
# Ingestion 1: 10:05 data → Inserted
# Ingestion 2: 10:00 data → Inserted (different snapshot_time)
```

**No special handling needed** - unique constraint operates on snapshot_time, not ingestion order

---

### Case 3: Source Data Corrections

**Scenario**: CoinGecko corrects Bitcoin's price from $87,000 to $86,500 retroactively

**Current Behavior**: Original value preserved (immutable snapshots)

**If Updates Required**:
```python
# Option 1: Soft delete with flag
UPDATE price_snapshots 
SET is_deleted = TRUE 
WHERE crypto_id = 'bitcoin' AND snapshot_time = '2025-12-25 10:00:00';

# Option 2: Hard update (use cautiously)
UPDATE price_snapshots 
SET current_price = 86500, 
    correction_applied = TRUE,
    corrected_at = CURRENT_TIMESTAMP
WHERE crypto_id = 'bitcoin' AND snapshot_time = '2025-12-25 10:00:00';
```

**Recommendation**: Treat historical snapshots as immutable; implement correction tracking if needed

---

### Case 4: Multiple Data Sources

**Scenario**: Ingesting from both CoinGecko and CoinMarketCap

**Solution**: Source-aware deduplication
```sql
-- Add source to unique constraint
ALTER TABLE price_snapshots 
ADD COLUMN source VARCHAR(50) NOT NULL DEFAULT 'CoinGecko';

-- Update unique constraint
ALTER TABLE price_snapshots 
DROP CONSTRAINT uq_price_snapshots_crypto_time;

ALTER TABLE price_snapshots 
ADD CONSTRAINT uq_price_snapshots_crypto_time_source 
UNIQUE (crypto_id, snapshot_time, source);
```

**Benefits**:
- Allows different sources for the same time period
- Enables price comparison across sources
- Maintains source attribution

---

## Query Patterns for Deduplicated Data

### Get Latest Price for Each Cryptocurrency
```sql
SELECT 
    c.crypto_id,
    c.symbol,
    c.name,
    ps.current_price,
    ps.snapshot_time
FROM cryptocurrencies c
INNER JOIN price_snapshots ps ON c.crypto_id = ps.crypto_id
WHERE ps.snapshot_time = (
    SELECT MAX(snapshot_time)
    FROM price_snapshots ps2
    WHERE ps2.crypto_id = ps.crypto_id
)
ORDER BY ps.current_price DESC;

-- Or use the view
SELECT * FROM v_latest_prices;
```

### Get Hourly Average Prices
```sql
SELECT 
    crypto_id,
    date_trunc('hour', snapshot_time) AS hour,
    AVG(current_price) AS avg_price,
    MIN(current_price) AS min_price,
    MAX(current_price) AS max_price,
    COUNT(*) AS snapshot_count
FROM price_snapshots
WHERE crypto_id = 'bitcoin'
  AND snapshot_time >= NOW() - INTERVAL '24 hours'
GROUP BY crypto_id, date_trunc('hour', snapshot_time)
ORDER BY hour DESC;
```

### Identify Duplicate Detection Events
```sql
-- Find how many ingestions were skipped due to deduplication
SELECT 
    ib.batch_id,
    ib.ingested_at,
    ib.record_count AS total_records,
    COUNT(ps.snapshot_id) AS inserted_snapshots,
    ib.record_count - COUNT(ps.snapshot_id) AS skipped_duplicates
FROM ingestion_batches ib
LEFT JOIN price_snapshots ps ON ib.batch_id = ps.batch_id
GROUP BY ib.batch_id, ib.ingested_at, ib.record_count
ORDER BY ib.ingested_at DESC;
```

---

## Performance Considerations

### Index Strategy
```sql
-- Essential indexes for deduplication
CREATE UNIQUE INDEX idx_price_snapshots_crypto_time 
ON price_snapshots(crypto_id, snapshot_time);

-- Query optimization indexes
CREATE INDEX idx_price_snapshots_snapshot_time 
ON price_snapshots(snapshot_time DESC);

CREATE INDEX idx_price_snapshots_crypto_time_desc 
ON price_snapshots(crypto_id, snapshot_time DESC);
```

### Partitioning for Large Datasets
```sql
-- Monthly partitioning for time-series data
CREATE TABLE price_snapshots_2025_12 PARTITION OF price_snapshots
FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE price_snapshots_2026_01 PARTITION OF price_snapshots
FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

---

## Monitoring and Validation

### Deduplication Metrics
```sql
-- Daily deduplication summary
CREATE VIEW v_deduplication_stats AS
SELECT 
    DATE(ib.ingested_at) AS ingestion_date,
    COUNT(DISTINCT ib.batch_id) AS total_batches,
    SUM(ib.record_count) AS total_records_ingested,
    COUNT(ps.snapshot_id) AS unique_snapshots_created,
    SUM(ib.record_count) - COUNT(ps.snapshot_id) AS duplicates_prevented,
    ROUND(
        100.0 * (SUM(ib.record_count) - COUNT(ps.snapshot_id)) / 
        NULLIF(SUM(ib.record_count), 0), 
        2
    ) AS duplicate_percentage
FROM ingestion_batches ib
LEFT JOIN price_snapshots ps ON ib.batch_id = ps.batch_id
GROUP BY DATE(ib.ingested_at)
ORDER BY ingestion_date DESC;
```

### Data Quality Checks
```python
def validate_deduplication(cursor):
    """Run data quality checks on deduplicated data."""
    
    # Check 1: No duplicate snapshots
    cursor.execute("""
        SELECT crypto_id, snapshot_time, COUNT(*)
        FROM price_snapshots
        GROUP BY crypto_id, snapshot_time
        HAVING COUNT(*) > 1;
    """)
    duplicates = cursor.fetchall()
    assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate snapshots"
    
    # Check 2: All snapshots aligned to interval
    cursor.execute("""
        SELECT snapshot_id, snapshot_time
        FROM price_snapshots
        WHERE EXTRACT(SECOND FROM snapshot_time) != 0
           OR EXTRACT(MINUTE FROM snapshot_time) % 5 != 0;
    """)
    misaligned = cursor.fetchall()
    assert len(misaligned) == 0, f"Found {len(misaligned)} misaligned snapshots"
    
    # Check 3: All market metrics have corresponding snapshots
    cursor.execute("""
        SELECT mm.metric_id
        FROM market_metrics mm
        LEFT JOIN price_snapshots ps ON mm.snapshot_id = ps.snapshot_id
        WHERE ps.snapshot_id IS NULL;
    """)
    orphaned = cursor.fetchall()
    assert len(orphaned) == 0, f"Found {len(orphaned)} orphaned metrics"
    
    print("✅ All deduplication validations passed")
```

---

## Configuration and Tuning

### Environment-Specific Settings

**Development**:
- Snapshot interval: 1 minute (more granular data for testing)
- Retain all batches
- Enable verbose logging

**Production**:
- Snapshot interval: 5 minutes (balance freshness and storage)
- Batch retention: 90 days (compliance)
- Error logging only

### Storage Savings Estimation

Assuming:
- 100 cryptocurrencies tracked
- Ingestion every 60 seconds
- 5-minute snapshot interval

**Without Deduplication**:
- Records/day: 100 crypto × 60 min/hour × 24 hours = 144,000 records
- Records/year: ~52.5 million records

**With 5-Minute Deduplication**:
- Records/day: 100 crypto × 12 snapshots/hour × 24 hours = 28,800 records
- Records/year: ~10.5 million records
- **Storage reduction: 80%**

---

## Best Practices

1. **Choose Appropriate Interval**: Balance data freshness with storage costs
2. **Monitor Duplicate Rates**: High rates may indicate interval misconfiguration
3. **Preserve Audit Trail**: Never delete ingestion_batches records
4. **Use Database Functions**: Ensure consistent time rounding across applications
5. **Document Changes**: Log any modifications to deduplication logic
6. **Test Edge Cases**: Validate behavior with late-arriving and out-of-order data
7. **Regular Validation**: Run quality checks to ensure deduplication integrity

---

## Future Enhancements

1. **Adaptive Intervals**: Adjust snapshot intervals based on volatility
2. **Change Detection**: Only store snapshots when significant price changes occur
3. **Compression**: Apply database compression for historical data
4. **Archival Strategy**: Move old snapshots to cheaper storage tiers
5. **Multi-Source Reconciliation**: Implement consensus mechanisms for multiple sources

---

## Summary

The deduplication strategy implements a multi-layered approach:

- **Master data**: UPSERT to maintain single source of truth
- **Time-series data**: Time-bucketed snapshots with unique constraints
- **Metrics**: Inherited deduplication via parent relationships
- **Audit trail**: No deduplication to preserve complete history

This strategy achieves:
- ✅ 80%+ storage reduction
- ✅ No duplicate snapshots
- ✅ Complete audit trail
- ✅ Query performance optimization
- ✅ Data consistency and integrity
