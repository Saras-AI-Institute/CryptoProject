"""
Generate ASCII and Mermaid ER diagrams for the crypto data pipeline.
Run this to get different diagram formats for documentation.
"""

def print_ascii_diagram():
    """Print ASCII art ER diagram."""
    diagram = """
╔═══════════════════════════════════════════════════════════════════════════════╗
║                    CRYPTO DATA PIPELINE - ER DIAGRAM (3NF)                     ║
╚═══════════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────┐
│   INGESTION_BATCHES             │
│─────────────────────────────────│
│ 🔑 batch_id (SERIAL) PK         │
│    ingested_at (TIMESTAMP)      │
│    source (VARCHAR)             │
│    record_count (INTEGER)       │
│    status (VARCHAR)             │
│    created_at (TIMESTAMP)       │
└────────────┬────────────────────┘
             │
             │ 1:N (tracks which batch discovered/updated crypto)
             │
             ▼
┌─────────────────────────────────┐
│   CRYPTOCURRENCIES              │
│─────────────────────────────────│
│ 🔑 crypto_id (VARCHAR) PK       │
│    symbol (VARCHAR) UQ          │
│    name (VARCHAR)               │
│    image_url (VARCHAR)          │
│    created_at (TIMESTAMP)       │
│    updated_at (TIMESTAMP)       │
└────────────┬────────────────────┘
             │
             │ 1:N (one crypto has many price snapshots over time)
             │
             ▼
┌─────────────────────────────────┐              ┌─────────────────────────────┐
│   PRICE_SNAPSHOTS               │              │   INGESTION_BATCHES         │
│─────────────────────────────────│              └──────────┬──────────────────┘
│ 🔑 snapshot_id (SERIAL) PK      │                         │
│ 🔗 crypto_id (VARCHAR) FK       │◄────────────────────────┘
│ 🔗 batch_id (INTEGER) FK        │              1:N (one batch creates many snapshots)
│    current_price (DECIMAL)      │
│    high_24h (DECIMAL)           │
│    low_24h (DECIMAL)            │
│    price_change_24h (DECIMAL)   │
│    price_change_pct_24h (DEC)   │
│    ath (DECIMAL)                │
│    ath_change_pct (DECIMAL)     │
│    ath_date (TIMESTAMP)         │
│    atl (DECIMAL)                │
│    atl_change_pct (DECIMAL)     │
│    atl_date (TIMESTAMP)         │
│    last_updated (TIMESTAMP)     │
│    snapshot_time (TIMESTAMP)    │
│    created_at (TIMESTAMP)       │
│ 🎯 UQ: (crypto_id, snapshot_time)│
└────────────┬────────────────────┘
             │
             │ 1:1 (each snapshot has one set of market metrics)
             │
             ▼
┌─────────────────────────────────┐
│   MARKET_METRICS                │
│─────────────────────────────────│
│ 🔑 metric_id (SERIAL) PK        │
│ 🔗 snapshot_id (INTEGER) FK UQ  │
│    market_cap (BIGINT)          │
│    market_cap_rank (INTEGER)    │
│    fully_diluted_val (BIGINT)   │
│    total_volume (BIGINT)        │
│    market_cap_change_24h        │
│    market_cap_change_pct_24h    │
│    circulating_supply (DECIMAL) │
│    total_supply (DECIMAL)       │
│    max_supply (DECIMAL)         │
│    roi_times (DECIMAL)          │
│    roi_currency (VARCHAR)       │
│    roi_percentage (DECIMAL)     │
│    created_at (TIMESTAMP)       │
└─────────────────────────────────┘

╔═══════════════════════════════════════════════════════════════════════════════╗
║                              KEY RELATIONSHIPS                                 ║
╚═══════════════════════════════════════════════════════════════════════════════╝

1. INGESTION_BATCHES → CRYPTOCURRENCIES (1:N implicit)
   - Batches track when cryptos were discovered/updated
   
2. CRYPTOCURRENCIES → PRICE_SNAPSHOTS (1:N)
   - One cryptocurrency has many historical price points
   - Foreign Key: price_snapshots.crypto_id → cryptocurrencies.crypto_id
   
3. INGESTION_BATCHES → PRICE_SNAPSHOTS (1:N)
   - One batch creates multiple price snapshots
   - Foreign Key: price_snapshots.batch_id → ingestion_batches.batch_id
   
4. PRICE_SNAPSHOTS → MARKET_METRICS (1:1)
   - Each price snapshot has exactly one set of market metrics
   - Foreign Key: market_metrics.snapshot_id → price_snapshots.snapshot_id
   - Unique constraint ensures 1:1 relationship

╔═══════════════════════════════════════════════════════════════════════════════╗
║                              NORMALIZATION (3NF)                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝

✅ 1NF: All attributes are atomic (no arrays or nested structures)
✅ 2NF: No partial dependencies (all non-key attributes depend on entire PK)
✅ 3NF: No transitive dependencies (no non-key attribute depends on another)

Separation Rationale:
- CRYPTOCURRENCIES: Static/slowly changing master data
- PRICE_SNAPSHOTS: Time-series price and historical performance
- MARKET_METRICS: Market-specific metrics (separated for clarity and flexibility)
- INGESTION_BATCHES: Audit trail (completely independent lifecycle)

╔═══════════════════════════════════════════════════════════════════════════════╗
║                              DEDUPLICATION                                     ║
╚═══════════════════════════════════════════════════════════════════════════════╝

🔒 CRYPTOCURRENCIES: UPSERT on crypto_id (updates metadata, no duplicates)
🔒 PRICE_SNAPSHOTS: Unique (crypto_id, snapshot_time) - time-bucketed to 5min
🔒 MARKET_METRICS: Inherits from PRICE_SNAPSHOTS via 1:1 relationship
🔒 INGESTION_BATCHES: No deduplication (every event is unique for audit)

📊 Storage Savings: ~80% reduction through time-bucketing deduplication
"""
    print(diagram)


def print_mermaid_diagram():
    """Print Mermaid.js ER diagram syntax."""
    print("\n" + "="*80)
    print("MERMAID DIAGRAM (paste into Mermaid Live Editor: https://mermaid.live)")
    print("="*80 + "\n")
    
    mermaid = """erDiagram
    INGESTION_BATCHES ||--o{ PRICE_SNAPSHOTS : creates
    CRYPTOCURRENCIES ||--o{ PRICE_SNAPSHOTS : has
    PRICE_SNAPSHOTS ||--|| MARKET_METRICS : contains
    
    INGESTION_BATCHES {
        serial batch_id PK
        timestamp ingested_at
        varchar source
        integer record_count
        varchar status
        timestamp created_at
    }
    
    CRYPTOCURRENCIES {
        varchar crypto_id PK
        varchar symbol UK
        varchar name
        varchar image_url
        timestamp created_at
        timestamp updated_at
    }
    
    PRICE_SNAPSHOTS {
        serial snapshot_id PK
        varchar crypto_id FK
        integer batch_id FK
        decimal current_price
        decimal high_24h
        decimal low_24h
        decimal price_change_24h
        decimal price_change_pct_24h
        decimal ath
        decimal ath_change_pct
        timestamp ath_date
        decimal atl
        decimal atl_change_pct
        timestamp atl_date
        timestamp last_updated
        timestamp snapshot_time UK
        timestamp created_at
    }
    
    MARKET_METRICS {
        serial metric_id PK
        integer snapshot_id FK_UK
        bigint market_cap
        integer market_cap_rank
        bigint fully_diluted_valuation
        bigint total_volume
        bigint market_cap_change_24h
        decimal market_cap_change_pct_24h
        decimal circulating_supply
        decimal total_supply
        decimal max_supply
        decimal roi_times
        varchar roi_currency
        decimal roi_percentage
        timestamp created_at
    }
"""
    print(mermaid)


def print_data_flow():
    """Print data flow diagram."""
    print("\n" + "="*80)
    print("DATA FLOW DIAGRAM")
    print("="*80 + "\n")
    
    flow = """
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION FLOW                              │
└─────────────────────────────────────────────────────────────────────────┘

    ┌─────────────────┐
    │  CoinGecko API  │
    │  (External)     │
    └────────┬────────┘
             │
             │ HTTP GET /api/v3/coins/markets
             │
             ▼
    ┌─────────────────┐
    │   ingest.py     │
    │ (Data Fetcher)  │
    └────────┬────────┘
             │
             │ Writes JSON file
             │
             ▼
    ┌─────────────────────────────┐
    │  landing_zone/              │
    │  crypto_prices_sample.json  │
    │  (Raw Data Storage)         │
    └────────┬────────────────────┘
             │
             │ Reads JSON
             │
             ▼
    ┌─────────────────┐
    │  load_data.py   │
    │ (ETL Process)   │
    └────────┬────────┘
             │
             │ 1. Parse JSON
             │ 2. Round timestamps (deduplication)
             │ 3. Transform nested structures
             │ 4. Validate data
             │
             ▼
    ┌─────────────────────────────────────────┐
    │         PostgreSQL Database             │
    │─────────────────────────────────────────│
    │                                         │
    │  ┌──────────────────────────────────┐  │
    │  │  1. INGESTION_BATCHES            │  │
    │  │     (Create batch record)        │  │
    │  └──────────────────────────────────┘  │
    │                 │                       │
    │                 ▼                       │
    │  ┌──────────────────────────────────┐  │
    │  │  2. CRYPTOCURRENCIES             │  │
    │  │     (UPSERT master data)         │  │
    │  └──────────────────────────────────┘  │
    │                 │                       │
    │                 ▼                       │
    │  ┌──────────────────────────────────┐  │
    │  │  3. PRICE_SNAPSHOTS              │  │
    │  │     (INSERT with dedup check)    │  │
    │  └──────────────────────────────────┘  │
    │                 │                       │
    │                 ▼                       │
    │  ┌──────────────────────────────────┐  │
    │  │  4. MARKET_METRICS               │  │
    │  │     (INSERT linked metrics)      │  │
    │  └──────────────────────────────────┘  │
    │                 │                       │
    │                 ▼                       │
    │  ┌──────────────────────────────────┐  │
    │  │  5. Update Batch Status          │  │
    │  │     (Mark as completed)          │  │
    │  └──────────────────────────────────┘  │
    └─────────────────────────────────────────┘
                     │
                     │ COMMIT transaction
                     │
                     ▼
    ┌─────────────────────────────────────────┐
    │     Analytics & Reporting Layer         │
    │─────────────────────────────────────────│
    │  • v_latest_prices (view)               │
    │  • v_price_history (view)               │
    │  • Custom SQL queries                   │
    │  • BI tools (Grafana, Metabase, etc.)   │
    └─────────────────────────────────────────┘
"""
    print(flow)


def print_deduplication_flow():
    """Print deduplication logic flow."""
    print("\n" + "="*80)
    print("DEDUPLICATION FLOW")
    print("="*80 + "\n")
    
    dedup = """
┌─────────────────────────────────────────────────────────────────────────┐
│                    TIME-BUCKETING DEDUPLICATION                          │
└─────────────────────────────────────────────────────────────────────────┘

Input: Bitcoin price @ 2025-12-26 10:03:47
                │
                ▼
    ┌───────────────────────────┐
    │ Round to 5-min interval   │
    │ using round_to_snapshot   │
    │ _interval()               │
    └───────────┬───────────────┘
                │
                ▼
    Snapshot Time: 2025-12-26 10:05:00
                │
                ▼
    ┌───────────────────────────┐
    │ Check if snapshot exists  │
    │ WHERE crypto_id = 'btc'   │
    │   AND snapshot_time =     │
    │       '2025-12-26 10:05'  │
    └───────────┬───────────────┘
                │
        ┌───────┴────────┐
        │                │
        ▼                ▼
   EXISTS           NOT EXISTS
        │                │
        │                │
        ▼                ▼
┌──────────────┐  ┌──────────────┐
│ ON CONFLICT  │  │ INSERT into  │
│ DO NOTHING   │  │ price_       │
│              │  │ snapshots    │
│ Skip insert  │  │ table        │
│ Log as       │  │              │
│ duplicate    │  │ Log as new   │
└──────────────┘  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ INSERT into  │
                  │ market_      │
                  │ metrics      │
                  └──────────────┘

╔═══════════════════════════════════════════════════════════════════════╗
║                         EXAMPLE TIMELINE                               ║
╚═══════════════════════════════════════════════════════════════════════╝

Time        Ingestion               Snapshot Time      Action
────────────────────────────────────────────────────────────────────────
10:02:30    Bitcoin @ $87,000  →    10:00:00           ✅ INSERT
10:03:45    Bitcoin @ $87,100  →    10:05:00           ✅ INSERT
10:04:20    Bitcoin @ $87,200  →    10:05:00           ❌ SKIP (duplicate)
10:06:15    Bitcoin @ $87,300  →    10:05:00           ❌ SKIP (duplicate)
10:07:50    Bitcoin @ $87,350  →    10:10:00           ✅ INSERT

Result: 5 ingestions → 3 unique snapshots (60% deduplication rate)
"""
    print(dedup)


if __name__ == "__main__":
    print_ascii_diagram()
    print_mermaid_diagram()
    print_data_flow()
    print_deduplication_flow()
    
    print("\n" + "="*80)
    print("✅ All diagrams generated successfully!")
    print("="*80)
