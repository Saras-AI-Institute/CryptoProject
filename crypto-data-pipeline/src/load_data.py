"""
Crypto Data Pipeline - Data Loader
===================================
Loads raw JSON crypto price data into normalized PostgreSQL database.

Features:
- Transforms nested JSON into normalized 3NF schema
- Implements deduplication strategy for time-series data
- Applies data quality validation before loading
- Handles database transactions for data integrity
- Supports both single file and batch processing
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when an inbound record fails data quality checks."""


def _validate_positive(value, field_name, allow_zero=True):
    """Ensure numeric value is non-negative (or strictly positive when required)."""
    if value is None:
        return
    if value < 0 or (not allow_zero and value == 0):
        raise DataQualityError(f"{field_name} has invalid value {value}")


def _validate_required_strings(record: Dict, fields: List[str]):
    """Ensure required string fields exist and are non-empty."""
    for field in fields:
        if not record.get(field):
            raise DataQualityError(f"Missing required field '{field}'")


class CryptoDataLoader:
    """Load crypto price data into normalized database schema."""
    
    def __init__(self, connection_params: Dict[str, str]):
        """
        Initialize database connection.
        
        Args:
            connection_params: Database connection parameters
                {
                    'host': 'localhost',
                    'database': 'crypto_db',
                    'user': 'your_user',
                    'password': 'your_password',
                    'port': 5432
                }
        """
        self.conn_params = connection_params
        self.conn = None
        self.cursor = None
        logger.debug("Initialized loader with host=%s database=%s", connection_params.get('host'), connection_params.get('database'))
        
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type:
            if self.conn:
                self.conn.rollback()
                logger.error("Transaction rolled back due to error")
        self.disconnect()
    
    def round_to_snapshot_interval(
        self, 
        timestamp: datetime, 
        interval_minutes: int = 5
    ) -> datetime:
        """
        Round timestamp to nearest interval for deduplication.
        
        Args:
            timestamp: Original timestamp
            interval_minutes: Rounding interval in minutes
            
        Returns:
            Rounded timestamp
        """
        # Use database function for consistency
        query = """
            SELECT round_to_snapshot_interval(%s::TIMESTAMP, %s);
        """
        self.cursor.execute(query, (timestamp, interval_minutes))
        return self.cursor.fetchone()[0]
    
    def create_ingestion_batch(
        self, 
        ingested_at: datetime, 
        source: str, 
        record_count: int
    ) -> int:
        """
        Create a new ingestion batch record.
        
        Args:
            ingested_at: Ingestion timestamp
            source: Data source name
            record_count: Number of records in batch
            
        Returns:
            batch_id
        """
        query = """
            INSERT INTO ingestion_batches (ingested_at, source, record_count, status)
            VALUES (%s, %s, %s, 'pending')
            RETURNING batch_id;
        """
        self.cursor.execute(query, (ingested_at, source, record_count))
        batch_id = self.cursor.fetchone()[0]
        logger.info(f"Created ingestion batch {batch_id}")
        return batch_id
    
    def upsert_cryptocurrency(self, record: Dict) -> str:
        """
        Insert or update cryptocurrency master data.
        
        Args:
            record: Cryptocurrency data from API
            
        Returns:
            crypto_id
        """
        query = """
            INSERT INTO cryptocurrencies (crypto_id, symbol, name, image_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (crypto_id) 
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                image_url = EXCLUDED.image_url,
                updated_at = CURRENT_TIMESTAMP
            RETURNING crypto_id;
        """
        
        crypto_id = record['id']
        symbol = record['symbol']
        name = record['name']
        image_url = record.get('image')
        
        self.cursor.execute(query, (crypto_id, symbol, name, image_url))
        return self.cursor.fetchone()[0]
    
    def insert_price_snapshot(
        self, 
        crypto_id: str, 
        batch_id: int, 
        record: Dict,
        snapshot_time: datetime
    ) -> Optional[int]:
        """
        Insert price snapshot (with deduplication).
        
        Args:
            crypto_id: Cryptocurrency identifier
            batch_id: Ingestion batch ID
            record: Price data from API
            snapshot_time: Normalized snapshot timestamp
            
        Returns:
            snapshot_id if inserted, None if duplicate
        """
        query = """
            INSERT INTO price_snapshots (
                crypto_id, batch_id, current_price, high_24h, low_24h,
                price_change_24h, price_change_pct_24h,
                ath, ath_change_pct, ath_date,
                atl, atl_change_pct, atl_date,
                last_updated, snapshot_time
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (crypto_id, snapshot_time) DO NOTHING
            RETURNING snapshot_id;
        """
        
        # Parse datetime strings
        last_updated = datetime.fromisoformat(
            record['last_updated'].replace('Z', '+00:00')
        )
        ath_date = datetime.fromisoformat(
            record['ath_date'].replace('Z', '+00:00')
        ) if record.get('ath_date') else None
        atl_date = datetime.fromisoformat(
            record['atl_date'].replace('Z', '+00:00')
        ) if record.get('atl_date') else None
        
        self.cursor.execute(query, (
            crypto_id,
            batch_id,
            record['current_price'],
            record.get('high_24h'),
            record.get('low_24h'),
            record.get('price_change_24h'),
            record.get('price_change_percentage_24h'),
            record.get('ath'),
            record.get('ath_change_percentage'),
            ath_date,
            record.get('atl'),
            record.get('atl_change_percentage'),
            atl_date,
            last_updated,
            snapshot_time
        ))
        
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            logger.debug(
                f"Duplicate snapshot for {crypto_id} at {snapshot_time} - skipped"
            )
            return None
    
    @staticmethod
    def validate_record(record: Dict):
        """Validate inbound record for data quality compliance."""
        _validate_required_strings(record, ['id', 'symbol', 'name'])
        _validate_positive(record.get('current_price'), 'current_price', allow_zero=False)
        for field in ['high_24h', 'low_24h', 'ath', 'atl']:
            _validate_positive(record.get(field), field)
        for field in ['market_cap', 'fully_diluted_valuation', 'total_volume', 'circulating_supply', 'total_supply', 'max_supply']:
            _validate_positive(record.get(field), field)
        rank = record.get('market_cap_rank')
        if rank is not None and rank <= 0:
            raise DataQualityError(f"market_cap_rank must be greater than zero (found {rank})")

    def insert_market_metrics(self, snapshot_id: int, record: Dict):
        """
        Insert market metrics for a price snapshot.
        
        Args:
            snapshot_id: Associated price snapshot ID
            record: Market data from API
        """
        query = """
            INSERT INTO market_metrics (
                snapshot_id, market_cap, market_cap_rank,
                fully_diluted_valuation, total_volume,
                market_cap_change_24h, market_cap_change_pct_24h,
                circulating_supply, total_supply, max_supply,
                roi_times, roi_currency, roi_percentage
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        # Handle ROI (can be null or dict)
        roi = record.get('roi')
        roi_times = roi.get('times') if roi else None
        roi_currency = roi.get('currency') if roi else None
        roi_percentage = roi.get('percentage') if roi else None
        
        self.cursor.execute(query, (
            snapshot_id,
            record.get('market_cap'),
            record.get('market_cap_rank'),
            record.get('fully_diluted_valuation'),
            record.get('total_volume'),
            record.get('market_cap_change_24h'),
            record.get('market_cap_change_percentage_24h'),
            record.get('circulating_supply'),
            record.get('total_supply'),
            record.get('max_supply'),
            roi_times,
            roi_currency,
            roi_percentage
        ))
    
    def update_batch_status(self, batch_id: int, status: str):
        """
        Update ingestion batch status.
        
        Args:
            batch_id: Batch identifier
            status: New status (pending, completed, failed)
        """
        query = """
            UPDATE ingestion_batches 
            SET status = %s 
            WHERE batch_id = %s;
        """
        self.cursor.execute(query, (status, batch_id))
    
    def load_json_file(self, file_path: Path) -> Tuple[int, int]:
        """
        Load data from a JSON file into the database.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Tuple of (batch_id, records_loaded)
        """
        logger.info(f"Loading data from {file_path}")
        
        # Read JSON file
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        ingested_at = datetime.fromisoformat(
            data['ingested_at'].replace('Z', '+00:00')
        )
        source = data['source']
        records = data['records']
        record_count = len(records)
        
        try:
            # Create batch
            batch_id = self.create_ingestion_batch(
                ingested_at, source, record_count
            )
            
            # Calculate snapshot time (rounded for deduplication)
            snapshot_time = self.round_to_snapshot_interval(ingested_at)
            
            loaded_count = 0
            skipped_count = 0
            dq_failures = 0
            
            # Process each record
            for record in records:
                try:
                    self.validate_record(record)
                except DataQualityError as dq_err:
                    dq_failures += 1
                    logger.warning(
                        "Data quality failure for crypto_id=%s: %s", record.get('id'), dq_err
                    )
                    continue

                # Upsert cryptocurrency
                crypto_id = self.upsert_cryptocurrency(record)
                
                # Insert price snapshot
                snapshot_id = self.insert_price_snapshot(
                    crypto_id, batch_id, record, snapshot_time
                )
                
                if snapshot_id:
                    # Insert market metrics
                    self.insert_market_metrics(snapshot_id, record)
                    loaded_count += 1
                else:
                    skipped_count += 1
            
            # Update batch status
            self.update_batch_status(batch_id, 'completed')
            
            # Commit transaction
            self.conn.commit()
            
            logger.info(
                "Batch %s completed: %s loaded, %s skipped (duplicates), %s failed quality checks",
                batch_id,
                loaded_count,
                skipped_count,
                dq_failures,
            )
            
            return batch_id, loaded_count
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading data: {e}")
            if 'batch_id' in locals():
                self.update_batch_status(batch_id, 'failed')
                self.conn.commit()
            raise
    
    def load_directory(self, directory_path: Path) -> List[Tuple[int, int]]:
        """
        Load all JSON files from a directory.
        
        Args:
            directory_path: Path to directory containing JSON files
            
        Returns:
            List of (batch_id, records_loaded) tuples
        """
        results = []
        json_files = list(directory_path.glob('*.json'))
        
        logger.info(f"Found {len(json_files)} JSON files in {directory_path}")
        
        for json_file in json_files:
            try:
                result = self.load_json_file(json_file)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to load {json_file}: {e}")
                continue
        
        return results


def main():
    """Main execution function."""
    
    # Database connection parameters sourced from environment (fallbacks for local dev)
    conn_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'crypto_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 5432)),
    }

    if not conn_params['password']:
        raise ValueError('DB_PASSWORD environment variable must be set to load data.')
    
    # Input file path
    input_file = Path('landing_zone/crypto_prices_sample.json')
    
    # Load data
    with CryptoDataLoader(conn_params) as loader:
        batch_id, records_loaded = loader.load_json_file(input_file)
        print(f"✅ Successfully loaded batch {batch_id} with {records_loaded} records")


if __name__ == '__main__':
    main()
