"""
Loader: Bulk-load Parquet staging files into SQL Server DW staging tables
Uses SQLAlchemy for flexible DB connectivity with bulk upsert support
"""
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from config import STAGING_DB_CONN_STR, STAGING_DIR, STAGING_TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def get_engine():
    return create_engine(STAGING_DB_CONN_STR, fast_executemany=True, pool_size=5)


def create_staging_tables(engine) -> None:
    """Create staging tables if they don't exist."""
    ddl = {
        "staging_customers": """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_customers')
            CREATE TABLE staging_customers (
                customer_id    INT PRIMARY KEY,
                customer_name  NVARCHAR(255),
                email          NVARCHAR(255),
                phone          NVARCHAR(50),
                city           NVARCHAR(100),
                state          NVARCHAR(100),
                country        NVARCHAR(100),
                created_at     DATETIME2,
                updated_at     DATETIME2,
                loaded_at      DATETIME2 DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "staging_products": """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_products')
            CREATE TABLE staging_products (
                product_id     INT PRIMARY KEY,
                product_name   NVARCHAR(255),
                category       NVARCHAR(100),
                subcategory    NVARCHAR(100),
                unit_cost      DECIMAL(18,4),
                unit_price     DECIMAL(18,4),
                supplier       NVARCHAR(255),
                updated_at     DATETIME2,
                loaded_at      DATETIME2 DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "staging_sales": """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_sales')
            CREATE TABLE staging_sales (
                sale_id        BIGINT PRIMARY KEY,
                customer_id    INT,
                product_id     INT,
                location_id    INT,
                quantity       INT,
                unit_price     DECIMAL(18,4),
                total_amount   DECIMAL(18,4),
                sale_date      DATETIME2,
                updated_at     DATETIME2,
                loaded_at      DATETIME2 DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "staging_locations": """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_locations')
            CREATE TABLE staging_locations (
                location_id    INT PRIMARY KEY,
                location_name  NVARCHAR(255),
                city           NVARCHAR(100),
                state          NVARCHAR(100),
                country        NVARCHAR(100),
                updated_at     DATETIME2,
                loaded_at      DATETIME2 DEFAULT CURRENT_TIMESTAMP
            );
        """,
    }

    with engine.begin() as conn:
        for table, sql in ddl.items():
            conn.execute(text(sql))
            log.info(f"[setup] Ensured table exists: {table}")


def load_parquet(table_name: str, parquet_path: str, engine) -> int:
    """
    Load a Parquet file into its corresponding staging table.
    Uses bulk insert with fast_executemany.
    Returns row count.
    """
    staging_table = STAGING_TABLES.get(table_name, f"staging_{table_name}")

    df = pd.read_parquet(parquet_path)
    if df.empty:
        log.info(f"[load:{table_name}] Empty — no rows to load")
        return 0

    # Ensure columns match schema
    df = df.where(pd.notnull(df), None)

    log.info(f"[load:{table_name}] Bulk loading {len(df)} rows into {staging_table}")
    with engine.begin() as conn:
        df.to_sql(
            staging_table,
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    log.info(f"[load:{table_name}] Successfully loaded {len(df)} rows")
    return len(df)


def load_all(staging_dir: str = STAGING_DIR) -> dict[str, int]:
    """
    Load all Parquet files in staging dir into DW staging tables.
    Returns dict of table_name -> row_count.
    """
    engine = get_engine()
    create_staging_tables(engine)

    counts = {}
    staging_path = Path(staging_dir)

    for parquet_file in sorted(staging_path.glob("*.parquet")):
        table_name = parquet_file.stem  # e.g. "customers" from "customers.parquet"
        try:
            count = load_parquet(table_name, str(parquet_file), engine)
            counts[table_name] = count
        except Exception as e:
            log.error(f"[load:{table_name}] FAILED: {e}")
            raise

    engine.dispose()
    return counts


def truncate_staging() -> None:
    """Clear all staging tables (for full reload scenarios)."""
    engine = get_engine()
    with engine.begin() as conn:
        for table in STAGING_TABLES.values():
            conn.execute(text(f"TRUNCATE TABLE {table}"))
            log.info(f"[truncate] Cleared {table}")
    engine.dispose()


if __name__ == "__main__":
    counts = load_all()
    total = sum(counts.values())
    print(f"Loaded {total} total rows across {len(counts)} tables")
