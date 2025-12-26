# Crypto Data Pipeline

Production-grade ingestion, normalization, and analytics modeling for cryptocurrency prices. The solution lands CoinGecko data, enforces 3NF integrity in PostgreSQL, and projects a Dimensional (star) model for BI consumers.

## Architecture Overview

### Logical Flow

```
CoinGecko API
    ↓ (ingest.py)
Landing Zone JSON (Bronze)
    ↓ (load_data.py)
PostgreSQL 3NF schema (Silver)
    ↓ (star_schema_transformations.sql)
Analytics schema / Delta Gold tables
    ↓
BI, dashboards, notebooks
```

### Normalized Core (3NF)

```
ingestion_batches (audit trail)
    ↓
cryptocurrencies (reference data)
    ↓
price_snapshots (fact) → market_metrics (1:1)
```

See `ER_DIAGRAM.md` for full entity documentation and `DEDUPLICATION_STRATEGY.md` for time-bucketing logic.

### Gold Layer Star Schema

```
dim_date      dim_time
      \        /
       fact_crypto_price_metrics
      /
dim_cryptocurrency   dim_source
```

Artifacts:
- `STAR_SCHEMA_DIAGRAM.md` – dimensional design.
- `star_schema_transformations.sql` – materializes facts/dimensions in PostgreSQL (`analytics` schema).
- `databricks/crypto_star_schema_pipeline.ipynb` – Delta Lake implementation with Bronze → Silver → Gold merges.

## Platform Rationale

- **PostgreSQL (3NF core)**: ACID-compliant, easy to host, rich constraint support for deduplication and data quality.
- **Databricks / Delta Lake (Gold layer)**: Scales analytics workloads, supports MERGE semantics for idempotent star schema loads, integrates with Lakehouse tooling.
- **Cron-based automation (extensible)**: Simple scheduler to run ingestion + load locally; pattern can translate to Snowflake Tasks or Azure Data Factory triggers.

## Setup Instructions

1. **Python environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Secrets & environment variables**
   - `COINGECKO_API_KEY` *(optional – required for CoinGecko Pro)*
   - `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PORT`, `DB_PASSWORD`
   - Store them in `.env` (never commit) or a cloud secret store (Azure Key Vault, AWS Secrets Manager, etc.).

3. **Database bootstrap**
   ```bash
   psql -U postgres -c "CREATE DATABASE crypto_db;"
   psql -U postgres -d crypto_db -f create_tables.sql
   ```
   Optional analytics schema:
   ```bash
   psql -U postgres -d crypto_db -f star_schema_transformations.sql
   ```

4. **Run the pipeline locally**
   ```bash
   export DB_PASSWORD=... # or rely on .env
   python src/ingest.py
   python src/load_data.py
   ```
   The helper script `automation/run_pipeline.sh` wraps both steps for scheduling.

5. **Databricks deployment**
   - Upload the notebook `databricks/crypto_star_schema_pipeline.ipynb`.
   - Configure DBFS mount/secret scopes for `raw_input_path` and database credentials.
   - Execute cells sequentially; the final cell validates duplicates and negative magnitudes.

## Data Quality & Monitoring

- **Pre-load validation**: `CryptoDataLoader.validate_record` rejects missing IDs, non-positive prices, or negative magnitudes before database insertion, logging failures per batch.
- **Database constraints**: `create_tables.sql` enforces positive prices, unique `(crypto_id, snapshot_time)` pairs, and referential integrity.
- **Notebook checks**: The Databricks quality cell fails the run if negative prices, negative market metrics, or null fact measures are detected.
- **Example log snippet**:
  ```text
  WARNING - Data quality failure for crypto_id=solana: current_price has invalid value -3.5
  INFO - Batch 12 completed: 95 loaded, 0 skipped (duplicates), 1 failed quality checks
  ```

## Security & Secrets Management

- API keys are injected at runtime via the `COINGECKO_API_KEY` environment variable (header `x-cg-pro-api-key`).
- Database credentials must be provided through environment variables; the loader refuses to run without `DB_PASSWORD`.
- For cloud deployments prefer secret scopes or managed services:
  - Azure Key Vault + Databricks secret scope.
  - AWS Secrets Manager with IAM roles.
  - Snowflake external functions with key rotation.

## Automation

### Cron (reference implementation)

1. Make the script executable: `chmod +x automation/run_pipeline.sh`.
2. Add a cron entry (runs every hour):
   ```cron
   0 * * * * cd /path/to/crypto-data-pipeline && ./automation/run_pipeline.sh >> logs/pipeline.log 2>&1
   ```

### Translating to Cloud Schedulers

- **Snowflake Tasks**: Wrap the SQL scripts in stored procedures and schedule via `CREATE TASK ... WAREHOUSE = ...`. Use the same deduplication logic and MERGE operations.
- **Azure Data Factory**: Orchestrate two activities (Notebook, Stored Procedure) with managed identity secrets referencing Key Vault.

## Usage Highlights

- **Ad-hoc ingestion**: `python src/ingest.py`
- **Load & dedupe**: `python src/load_data.py`
- **Latest prices**: `SELECT * FROM v_latest_prices;`
- **Historical slice**:
  ```sql
  SELECT symbol, snapshot_time, current_price, price_change_pct_24h
  FROM v_price_history
  WHERE symbol = 'btc'
  ORDER BY snapshot_time DESC
  LIMIT 10;
  ```
- **Star schema fact sample**:
  ```sql
  SELECT d.symbol, dd.full_date, AVG(f.current_price) AS avg_price
  FROM analytics.fact_crypto_price_metrics f
  JOIN analytics.dim_cryptocurrency d ON f.crypto_key = d.crypto_key
  JOIN analytics.dim_date dd ON f.date_key = dd.date_key
  GROUP BY d.symbol, dd.full_date
  ORDER BY dd.full_date DESC;
  ```

## Project Structure

```
crypto-data-pipeline/
├── README.md
├── PROJECT_SUMMARY.md
├── ER_DIAGRAM.md
├── STAR_SCHEMA_DIAGRAM.md
├── DEDUPLICATION_STRATEGY.md
├── create_tables.sql
├── star_schema_transformations.sql
├── requirements.txt
├── automation/
│   └── run_pipeline.sh
├── databricks/
│   └── crypto_star_schema_pipeline.ipynb
├── landing_zone/
│   └── crypto_prices_sample.json
└── src/
    ├── ingest.py
    └── load_data.py
```

## Challenges & Lessons Learned

- **API throttling**: CoinGecko rate limits required retry with exponential backoff and optional API key support.
- **Data anomalies**: Negative magnitudes surfaced in test data; added strict validation and logging to quarantine bad records without failing batches.
- **Schema evolution**: Separating 3NF core from Gold layer ensures constraints do not block analytical flexibility, while Delta MERGEs keep facts idempotent.

## Contributing & Support

1. Document schema updates in `ER_DIAGRAM.md` and `STAR_SCHEMA_DIAGRAM.md`.
2. Mirror any business logic tweaks across SQL and notebook implementations.
3. Prefer environment variables or secret managers for new connectors.

Questions? Review the documentation above or open an issue describing the desired enhancement.
