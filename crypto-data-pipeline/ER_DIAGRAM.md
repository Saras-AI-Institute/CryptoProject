# ER Diagram - Crypto Price Data Pipeline

## Entity-Relationship Diagram (3NF Normalized Schema)

```
┌─────────────────────────────┐
│   INGESTION_BATCHES         │
│─────────────────────────────│
│ PK: batch_id (SERIAL)       │
│     ingested_at (TIMESTAMP) │
│     source (VARCHAR)        │
│     record_count (INTEGER)  │
│     status (VARCHAR)        │
└──────────────┬──────────────┘
               │
               │ 1
               │
               │ N
┌──────────────▼──────────────┐
│   CRYPTOCURRENCIES          │
│─────────────────────────────│
│ PK: crypto_id (VARCHAR)     │
│     symbol (VARCHAR)        │
│     name (VARCHAR)          │
│     image_url (VARCHAR)     │
│     created_at (TIMESTAMP)  │
│     updated_at (TIMESTAMP)  │
└──────────────┬──────────────┘
               │
               │ 1
               │
               │ N
┌──────────────▼──────────────┐
│   PRICE_SNAPSHOTS           │
│─────────────────────────────│
│ PK: snapshot_id (SERIAL)    │
│ FK: crypto_id (VARCHAR)     │
│ FK: batch_id (INTEGER)      │
│     current_price (DECIMAL) │
│     high_24h (DECIMAL)      │
│     low_24h (DECIMAL)       │
│     price_change_24h (DEC)  │
│     price_change_pct_24h    │
│     ath (DECIMAL)           │
│     ath_change_pct (DECIMAL)│
│     ath_date (TIMESTAMP)    │
│     atl (DECIMAL)           │
│     atl_change_pct (DECIMAL)│
│     atl_date (TIMESTAMP)    │
│     last_updated (TIMESTAMP)│
│     snapshot_time (TIMESTAMP)│
│ UQ: (crypto_id, snapshot_time)│
└──────────────┬──────────────┘
               │
               │ 1:1
               │
┌──────────────▼──────────────┐
│   MARKET_METRICS            │
│─────────────────────────────│
│ PK: metric_id (SERIAL)      │
│ FK: snapshot_id (INTEGER)   │
│     market_cap (BIGINT)     │
│     market_cap_rank (INT)   │
│     fully_diluted_val (BIG) │
│     total_volume (BIGINT)   │
│     market_cap_change_24h   │
│     market_cap_change_pct   │
│     circulating_supply (DEC)│
│     total_supply (DECIMAL)  │
│     max_supply (DECIMAL)    │
└─────────────────────────────┘
```

## Normalization Analysis (3NF Compliance)

### 1NF (First Normal Form)
- ✅ All attributes contain atomic values
- ✅ Each column contains values of a single type
- ✅ Each column has a unique name
- ✅ Order doesn't matter

### 2NF (Second Normal Form)
- ✅ All non-key attributes are fully functionally dependent on the primary key
- ✅ No partial dependencies exist
- **Cryptocurrencies**: All attributes depend on `crypto_id`
- **Price Snapshots**: All attributes depend on `snapshot_id`
- **Market Metrics**: All attributes depend on `metric_id`
- **Ingestion Batches**: All attributes depend on `batch_id`

### 3NF (Third Normal Form)
- ✅ No transitive dependencies
- ✅ All non-key attributes depend only on the primary key
- **Separation rationale**:
  - Cryptocurrency static info (id, symbol, name) separated from time-series data
  - Price information separated from market metrics to avoid redundancy
  - Ingestion metadata isolated for audit trail

## Entity Descriptions

### INGESTION_BATCHES
**Purpose**: Track each data ingestion event for audit and lineage tracking

**Attributes**:
- `batch_id`: Auto-incrementing primary key
- `ingested_at`: Timestamp when batch was ingested
- `source`: Data source name (e.g., "CoinGecko")
- `record_count`: Number of records in batch
- `status`: Batch processing status (pending, completed, failed)

### CRYPTOCURRENCIES
**Purpose**: Store static/slowly changing cryptocurrency master data

**Attributes**:
- `crypto_id`: Unique identifier from source (e.g., "bitcoin")
- `symbol`: Trading symbol (e.g., "btc")
- `name`: Full name (e.g., "Bitcoin")
- `image_url`: URL to cryptocurrency image
- `created_at`: Record creation timestamp
- `updated_at`: Last update timestamp

**Deduplication**: Updates existing records based on `crypto_id`

### PRICE_SNAPSHOTS
**Purpose**: Store time-series price data and historical metrics

**Attributes**:
- `snapshot_id`: Auto-incrementing primary key
- `crypto_id`: Foreign key to CRYPTOCURRENCIES
- `batch_id`: Foreign key to INGESTION_BATCHES
- `current_price`: Current price in USD
- `high_24h`: 24-hour high
- `low_24h`: 24-hour low
- `price_change_24h`: Absolute price change
- `price_change_pct_24h`: Percentage price change
- `ath`: All-time high price
- `ath_change_pct`: Change from ATH
- `ath_date`: Date of ATH
- `atl`: All-time low price
- `atl_change_pct`: Change from ATL
- `atl_date`: Date of ATL
- `last_updated`: Source timestamp
- `snapshot_time`: Normalized snapshot time (rounded to nearest interval)

**Deduplication**: Unique constraint on `(crypto_id, snapshot_time)`

### MARKET_METRICS
**Purpose**: Store market capitalization and supply metrics

**Attributes**:
- `metric_id`: Auto-incrementing primary key
- `snapshot_id`: Foreign key to PRICE_SNAPSHOTS (1:1 relationship)
- `market_cap`: Market capitalization
- `market_cap_rank`: Ranking by market cap
- `fully_diluted_val`: Fully diluted valuation
- `total_volume`: 24-hour trading volume
- `market_cap_change_24h`: Market cap change
- `market_cap_change_pct`: Market cap change percentage
- `circulating_supply`: Coins in circulation
- `total_supply`: Total coins issued
- `max_supply`: Maximum possible supply

**Deduplication**: Inherits from PRICE_SNAPSHOTS via 1:1 relationship

## Relationships

1. **INGESTION_BATCHES → CRYPTOCURRENCIES** (1:N)
   - One batch can discover/update multiple cryptocurrencies

2. **CRYPTOCURRENCIES → PRICE_SNAPSHOTS** (1:N)
   - One cryptocurrency has many price snapshots over time

3. **INGESTION_BATCHES → PRICE_SNAPSHOTS** (1:N)
   - One batch creates multiple price snapshots

4. **PRICE_SNAPSHOTS → MARKET_METRICS** (1:1)
   - Each price snapshot has corresponding market metrics

## Indexes for Query Performance

- `idx_price_snapshots_crypto_time`: (crypto_id, snapshot_time) - Time-series queries
- `idx_price_snapshots_batch`: (batch_id) - Batch lineage queries
- `idx_market_metrics_snapshot`: (snapshot_id) - Join optimization
- `idx_cryptocurrencies_symbol`: (symbol) - Symbol lookups
- `idx_price_snapshots_last_updated`: (last_updated) - Recent data queries
