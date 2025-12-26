# Crypto Data Pipeline - Project Summary

## 📋 Project Overview

This project implements a production-ready cryptocurrency data pipeline with a fully normalized (3NF) PostgreSQL database schema. The pipeline ingests real-time crypto price data from CoinGecko API, applies intelligent deduplication strategies, and stores the data in a normalized relational database optimized for time-series analytics.

## ✅ Deliverables Completed

### 1. ER Diagram (3NF Normalized Schema) ✓

**File**: `ER_DIAGRAM.md`

**Features**:
- Visual ASCII diagram showing all entities and relationships
- Detailed normalization analysis (1NF, 2NF, 3NF compliance)
- Comprehensive entity descriptions with all attributes
- Index strategy documentation
- Query performance optimization notes

**Entities Designed**:
- `ingestion_batches` - Audit trail for data lineage
- `cryptocurrencies` - Master reference data
- `price_snapshots` - Time-series price data
- `market_metrics` - Market capitalization and supply metrics

**Relationships**:
- 1:N from ingestion_batches to price_snapshots
- 1:N from cryptocurrencies to price_snapshots  
- 1:1 from price_snapshots to market_metrics

### 2. SQL Table Creation Scripts ✓

**File**: `../sql/create_tables.sql`

**Features**:
- Complete DDL for all normalized tables
- Foreign key constraints for referential integrity
- Unique constraints for deduplication
- Check constraints for data validation
- Strategic indexes for query optimization
- Useful views for common queries
- Helper functions for timestamp rounding
- Automatic triggers for timestamp updates
- Comprehensive inline documentation

**Key SQL Objects**:
- 4 normalized tables
- 10+ indexes for performance
- 2 convenience views (v_latest_prices, v_price_history)
- 1 utility function (round_to_snapshot_interval)
- 1 trigger for auto-updating timestamps

### 3. Data Loading Script ✓

**File**: `src/load_data.py`

**Features**:
- Complete ETL pipeline for JSON to PostgreSQL
- Transaction-safe batch processing
- Intelligent deduplication using time-bucketing
- UPSERT patterns for master data
- Comprehensive error handling and logging
- Support for single file and directory batch processing
- Context manager for safe database connections

**Key Functions**:
- `create_ingestion_batch()` - Track data lineage
- `upsert_cryptocurrency()` - Update master data
- `insert_price_snapshot()` - Time-series with dedup
- `insert_market_metrics()` - Related metrics
- `load_json_file()` - Full ETL orchestration
- `load_directory()` - Batch processing

### 4. Deduplication Strategy Documentation ✓

**File**: `DEDUPLICATION_STRATEGY.md`

**Features**:
- Comprehensive strategy for each entity type
- Time-bucketing algorithm explanation
- Edge case handling (late data, corrections, multiple sources)
- Performance analysis and storage savings
- Query patterns for deduplicated data
- Monitoring and validation approaches
- Configuration recommendations per environment
- Best practices and future enhancements

**Strategy Summary**:
- **Cryptocurrencies**: UPSERT on crypto_id
- **Price Snapshots**: Time-bucketed unique constraint (5-min intervals)
- **Market Metrics**: Inherited via 1:1 relationship
- **Ingestion Batches**: No deduplication (audit trail)

**Results**: ~80% storage reduction with maintained data integrity

### 5. Star Schema & Databricks Gold Layer ✓

**Files**: `STAR_SCHEMA_DIAGRAM.md`, `../sql/star_schema_transformations.sql`, `databricks/crypto_star_schema_pipeline.ipynb`

**Features**:
- Dimensional model with fact table (`fact_crypto_price_metrics`) plus conformed dimensions
- Reusable SQL transforms that stage data into `analytics.*` tables inside PostgreSQL
- Databricks Delta notebook orchestrating Bronze → Silver → Gold merges with widgets and deduplication
- Automated surrogate keys and business-key based idempotent MERGEs
- Quality checks for duplicate facts and symbol freshness

**Highlights**:
- Maintains lineage via degenerate `batch_id` on the fact table
- Aligns snapshot grain with normalized 5-minute deduplication interval
- Supports downstream BI tools (Power BI, Tableau) with calendar/time dimensions

### 6. Data Quality, Security & Automation ✓

**Files**: `../src/load_data.py`, `databricks/crypto_star_schema_pipeline.ipynb`, `../src/run_pipeline.sh`, `../README.md`

**Features**:
- Pre-load validation rejects negative magnitudes, zero prices, and missing identifiers before database writes.
- Databricks notebook cell halts the job if Gold facts contain invalid numeric ranges or null measures.
- API authentication relies on the `COINGECKO_API_KEY` environment variable; database credentials sourced exclusively from `DB_*` env vars.
- Turn-key cron wrapper (`run_pipeline.sh`) with `.env` support for local or server-based scheduling.
- README now captures architecture diagrams, platform rationale, setup guidance, and key delivery challenges.

**Impact**:
- Increased trust in downstream analytics by surfacing invalid records early.
- Simplified secret rotation with zero hard-coded credentials.
- Provides a repeatable operations story (cron, Snowflake Tasks, ADF) without changing application code.

## 📁 Project Structure

```
crypto-data-pipeline/
├── README.md                      # Complete setup and usage guide
├── requirements.txt               # Python dependencies
├── docs/
│   ├── DEDUPLICATION_STRATEGY.md  # Deduplication approach ✓
│   ├── ER_DIAGRAM.md              # Entity-relationship documentation ✓
│   ├── PROJECT_SUMMARY.md         # This file
│   ├── STAR_SCHEMA_DIAGRAM.md     # Dimensional design ✓
│   └── databricks/
│       └── crypto_star_schema_pipeline.ipynb  # Delta Lake pipeline ✓
├── sql/
│   ├── create_tables.sql          # Database schema creation ✓
│   ├── example_queries.sql        # Common SQL query examples
│   └── star_schema_transformations.sql # Analytics star schema ✓
├── landing_zone/
│   └── crypto_prices_sample.json  # Sample ingested data
└── src/
    ├── generate_diagrams.py       # Visual diagram generator
    ├── ingest.py                  # API data fetcher
    ├── load_data.py               # ETL script ✓
    └── run_pipeline.sh            # Scheduled orchestration helper
```

## 🔑 Key Technical Decisions

### 1. Normalization to 3NF
**Decision**: Separate price data from market metrics and master data

**Rationale**:
- Eliminates redundancy
- Supports independent update frequencies
- Enables flexible querying
- Maintains referential integrity

### 2. Time-Bucketing Deduplication
**Decision**: Round timestamps to 5-minute intervals

**Rationale**:
- Prevents near-duplicate snapshots
- Predictable snapshot times for queries
- Significant storage savings (80%)
- Balances freshness with efficiency

### 3. First-Write-Wins for Snapshots
**Decision**: `ON CONFLICT DO NOTHING` for price snapshots

**Rationale**:
- Immutable historical records
- Simpler logic than last-write-wins
- Better performance (no updates)
- Clear audit trail via batch tracking

### 4. UPSERT for Master Data
**Decision**: Update cryptocurrency metadata on conflict

**Rationale**:
- Master data changes infrequently
- Need single source of truth
- Automatic sync with source
- Timestamp tracking for changes

## 📊 Database Schema Highlights

### Table Sizes (Estimated for 1 Year)

Assuming:
- 100 cryptocurrencies tracked
- Ingestion every 60 seconds
- 5-minute snapshot interval

| Table | Records/Year | Storage (approx) |
|-------|-------------|------------------|
| cryptocurrencies | 100 | < 1 MB |
| ingestion_batches | 525,600 | ~50 MB |
| price_snapshots | 10.5M | ~5 GB |
| market_metrics | 10.5M | ~4 GB |

**Without deduplication**: ~50M records, ~45 GB

### Critical Indexes

```sql
-- Deduplication enforcement
UNIQUE (crypto_id, snapshot_time) ON price_snapshots

-- Time-series queries
INDEX (crypto_id, snapshot_time DESC) ON price_snapshots

-- Latest price lookups  
INDEX (snapshot_time DESC) ON price_snapshots

-- Batch tracking
INDEX (batch_id) ON price_snapshots
```

## 🚀 Usage Examples

### Setup Database
```bash
psql -U postgres -d crypto_db -f sql/create_tables.sql
```

### Ingest Data
```bash
python src/ingest.py
```

### Load into Database
```bash
python src/load_data.py
```

### Query Latest Prices
```sql
SELECT * FROM v_latest_prices;
```

### Analyze Price History
```sql
SELECT 
    symbol,
    snapshot_time,
    current_price,
    price_change_pct_24h
FROM v_price_history
WHERE symbol = 'btc'
ORDER BY snapshot_time DESC
LIMIT 20;
```

## 🎯 Design Patterns Used

1. **Slowly Changing Dimension (Type 1)**: Cryptocurrencies table
2. **Time-Series Partitioning**: Ready for partitioning by snapshot_time
3. **Audit Trail Pattern**: Ingestion batches tracking
4. **Normalized Star Schema**: Fact (price_snapshots) with dimensions
5. **Idempotent Loads**: Rerunning loads won't create duplicates

## 🔍 Data Quality Features

### Constraints
- Primary keys on all tables
- Foreign keys for referential integrity
- Unique constraints for deduplication
- Check constraints for business rules
- NOT NULL on critical fields

### Validation
```sql
-- Price must be positive
CHECK (current_price > 0)

-- High must be >= Low
CHECK (high_24h >= low_24h)

-- Rank must be positive
CHECK (market_cap_rank > 0)

-- Supply hierarchy
CHECK (max_supply >= total_supply)
```

### Monitoring Queries
```sql
-- Check for duplicates
SELECT crypto_id, snapshot_time, COUNT(*)
FROM price_snapshots
GROUP BY crypto_id, snapshot_time
HAVING COUNT(*) > 1;

-- Data freshness
SELECT MAX(snapshot_time) AS latest_data
FROM price_snapshots;

-- Deduplication effectiveness
SELECT 
    SUM(record_count) AS total_ingested,
    COUNT(DISTINCT ps.snapshot_id) AS unique_snapshots,
    (1 - COUNT(DISTINCT ps.snapshot_id)::FLOAT / SUM(record_count)) * 100 AS dedup_pct
FROM ingestion_batches ib
LEFT JOIN price_snapshots ps ON ib.batch_id = ps.batch_id;
```

## 📈 Performance Optimization

### Index Strategy
- Covering indexes for common queries
- Composite indexes for time-series access
- Partial indexes for active data (if needed)

### Query Optimization
- Materialized views for complex aggregations
- Query-specific indexes
- Partitioning strategy for large datasets

### Future Scalability
```sql
-- Monthly partitioning template
CREATE TABLE price_snapshots_YYYY_MM PARTITION OF price_snapshots
FOR VALUES FROM ('YYYY-MM-01') TO ('YYYY-MM+1-01');
```

## 🧪 Testing & Validation

### Data Quality Tests
1. No duplicate snapshots within same interval
2. All timestamps aligned to 5-minute boundaries
3. All market metrics have corresponding snapshots
4. All foreign keys resolve correctly
5. Price values are positive and reasonable

### Run Validation
```python
from src.load_data import CryptoDataLoader

with CryptoDataLoader(conn_params) as loader:
    # Run quality checks from DEDUPLICATION_STRATEGY.md
    validate_deduplication(loader.cursor)
```

## 🔐 Security Considerations

### Database Permissions
```sql
-- Read-only analytics role
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_role;

-- ETL role with write access
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO etl_role;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO etl_role;
```

### Configuration Management
- Use environment variables for credentials
- Never commit connection strings to version control
- Implement connection pooling for production
- Use SSL for database connections

## 📝 Additional Documentation

### Generated Diagrams
Run `python generate_diagrams.py` for:
- ASCII ER diagram
- Mermaid.js syntax (for online rendering)
- Data flow diagram
- Deduplication flow visualization

### Example Queries
See `example_queries.sql` for:
- Latest price retrieval
- Time-series analysis
- Market analysis
- Historical performance
- Data quality monitoring
- Advanced analytics

## 🎉 Project Success Metrics

✅ **Normalization**: Full 3NF compliance  
✅ **Deduplication**: 80% storage reduction  
✅ **Data Integrity**: Complete referential integrity  
✅ **Query Performance**: Strategic indexing  
✅ **Audit Trail**: Full lineage tracking  
✅ **Documentation**: Comprehensive guides  
✅ **Production Ready**: Error handling and logging  
✅ **Extensibility**: Easy to add new data sources  

## 🔄 Next Steps (Future Enhancements)

1. **Automation**: Schedule with Airflow or cron
2. **Monitoring**: Implement alerting for data quality issues
3. **API Layer**: REST API for data access
4. **Visualization**: Grafana dashboards
5. **Real-time**: Kafka/streaming integration
6. **Multi-source**: Add CoinMarketCap, Binance APIs
7. **ML Features**: Price prediction models
8. **Data Warehouse**: Aggregate tables for reporting

## 📧 Support

All deliverables are complete and production-ready:
- ✅ ER Diagram with 3NF schema
- ✅ SQL table creation scripts
- ✅ Data loading script
- ✅ Deduplication strategy documentation

For questions or issues, refer to the comprehensive documentation in:
- `README.md` - Setup and usage
- `ER_DIAGRAM.md` - Schema design
- `DEDUPLICATION_STRATEGY.md` - Data handling
- `example_queries.sql` - Query patterns
