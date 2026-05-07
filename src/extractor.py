"""
Extractor: Incremental CDC from SQL Server via watermark columns
Saves extracted data as Parquet files for downstream Airflow transfer
"""
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyodbc

from config import SQLSERVER_CONN_STR, SOURCE_TABLES, STAGING_DIR, WATERMARK_FILE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def read_watermark() -> str:
    """Read last successful extraction timestamp."""
    try:
        with open(WATERMARK_FILE) as f:
            ts = f.read().strip()
            log.info(f"Watermark read: {ts}")
            return ts
    except FileNotFoundError:
        log.info("No watermark found — using epoch start")
        return "2000-01-01T00:00:00"


def write_watermark(ts: str) -> None:
    """Persist last successful extraction timestamp."""
    Path(WATERMARK_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        f.write(ts)
    log.info(f"Watermark updated: {ts}")


def extract_table(table_name: str, watermark: str) -> pd.DataFrame:
    """
    Extract rows updated since last watermark from a source table.
    Returns DataFrame (may be empty).
    """
    cfg = SOURCE_TABLES[table_name]
    query = cfg["query"].format(watermark=watermark)

    log.info(f"[extract:{table_name}] Connecting to SQL Server...")
    try:
        with pyodbc.connect(SQLSERVER_CONN_STR, timeout=30, appname="ETL-Extractor") as conn:
            df = pd.read_sql(query, conn)
    except pyodbc.Error as e:
        log.error(f"[extract:{table_name}] Connection error: {e}")
        raise

    log.info(f"[extract:{table_name}] Fetched {len(df)} rows")
    if not df.empty:
        log.info(
            f"[extract:{table_name}] Range: {df[cfg['watermark_col']].min()} → "
            f"{df[cfg['watermark_col']].max()}"
        )
    return df


def extract_all() -> dict[str, pd.DataFrame]:
    """
    Full incremental extraction run — all source tables.
    Returns dict of table_name -> DataFrame.
    """
    watermark = read_watermark()
    results = {}

    for table_name in SOURCE_TABLES:
        try:
            df = extract_table(table_name, watermark)
            results[table_name] = df
        except Exception as e:
            log.error(f"[extract:{table_name}] FAILED: {e}")
            raise

    # Determine new watermark: latest updated_at across all tables
    new_watermark = watermark
    for table_name, df in results.items():
        if not df.empty:
            col = SOURCE_TABLES[table_name]["watermark_col"]
            latest = pd.to_datetime(df[col]).max()
            if pd.to_datetime(new_watermark) < latest:
                new_watermark = str(latest)

    if new_watermark != watermark:
        write_watermark(new_watermark)
        log.info(f"Watermark advanced to: {new_watermark}")
    else:
        log.info("No new data — watermark unchanged")

    return results


def save_parquet(output_dir: str = STAGING_DIR) -> dict[str, str]:
    """
    Extract all tables and save as Parquet files.
    Returns dict of table_name -> file_path.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    data = extract_all()
    paths = {}

    for table_name, df in data.items():
        path = os.path.join(output_dir, f"{table_name}.parquet")
        df.to_parquet(path, index=False, compression="snappy")
        paths[table_name] = path
        log.info(f"[save:{table_name}] {len(df)} rows → {path}")

    return paths


if __name__ == "__main__":
    paths = save_parquet()
    print(f"Extracted {len(paths)} tables to {STAGING_DIR}")
