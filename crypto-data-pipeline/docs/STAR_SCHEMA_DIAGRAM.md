# Star Schema - Crypto Analytics

## Purpose

This star schema models cryptocurrency price observations for downstream analytics, reporting, and BI tools. It denormalizes the 3NF core tables (`ingestion_batches`, `cryptocurrencies`, `price_snapshots`, `market_metrics`) into a single fact table surrounded by conformed dimensions so analysts can slice measures by asset, date, time, and ingestion source.

## Diagram

```
                dim_date
                   |
                   |
  dim_source --- fact_crypto_price_metrics --- dim_cryptocurrency
                   |
                   |
                dim_time
```

### Grain Statement

`fact_crypto_price_metrics` stores one record per cryptocurrency and normalized snapshot timestamp (5-minute buckets) coming from the ingestion pipeline.

### Fact Table: `fact_crypto_price_metrics`

Key characteristics:
- **Surrogate Key**: `fact_id` (identity)
- **Foreign Keys**: `crypto_key`, `date_key`, `time_key`, `source_key`
- **Degenerate Dimension**: `batch_id` from ingestion batches retained on the fact to preserve lineage
- **Measures**: price and market metrics including current price, 24h high/low, price deltas, ATH/ATL changes, market cap, volume, supplies, ROI metrics

### Dimension Tables

| Dimension | Business Key | Notes |
|-----------|--------------|-------|
| `dim_cryptocurrency` | `crypto_id` | Type-1 SCD holding symbol, name, and image metadata |
| `dim_date` | `full_date` | Calendar attributes (day, month, quarter, year, weekday flags) |
| `dim_time` | `full_time` | Minute-level time-of-day breakdown for intraday analysis |
| `dim_source` | `source_name` | Ingestion system or upstream feed identifier |

### Lineage Mapping

- `dim_cryptocurrency` -> `cryptocurrencies`
- `dim_source` -> `ingestion_batches`
- `dim_date` and `dim_time` derive from `price_snapshots.snapshot_time`
- `fact_crypto_price_metrics` combines `price_snapshots` and `market_metrics`, joined with the dimensions above
### Usage Patterns

- Track price trends by day, hour, asset, and source
- Build Power BI/Tableau models on top of conformed dimensions
- Simplify aggregations (e.g., average price or total market cap by date)
- Support incremental Delta Lake merges or warehouse loads by snapshot timestamp

### Maintenance Notes

- Rebuild or incrementally update dimensions before processing the fact load
- Ensure snapshot timestamps remain aligned to the 5-minute deduplication interval to prevent duplicate facts
- Enforce unique constraint on `(crypto_key, snapshot_timestamp)` in the fact table to maintain idempotent loads
- Refresh downstream semantic models after each Gold-layer load
