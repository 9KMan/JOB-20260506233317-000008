# ETL Pipeline: SQL Server → Star Schema DW → Power BI

## Architecture

```
┌─────────────────┐     CDC Query      ┌──────────────────┐
│  SQL Server     │ ─── pyodbc ────→  │  Parquet Files   │
│  (Source OLTP)  │   Watermark col    │  /tmp/staging/   │
└─────────────────┘                    └────────┬─────────┘
                                                │ Airflow
                                                ▼
                                     ┌──────────────────┐
                                     │  Staging Tables  │
                                     │ (SQL Server DW)  │
                                     └────────┬─────────┘
                                                │ dbt run
                                                ▼
                                     ┌──────────────────┐
                                     │  Star Schema     │
                                     │  dim_customer    │
                                     │  dim_product     │
                                     │  dim_location    │
                                     │  dim_date        │
                                     │  fact_sales      │
                                     └────────┬─────────┘
                                                │ Power BI
                                                ▼
                                     ┌──────────────────┐
                                     │   Dashboards     │
                                     └──────────────────┘
```

## Stack

| Layer          | Technology                          |
|----------------|-------------------------------------|
| Orchestration  | Apache Airflow 2.9                 |
| Transformation | dbt Core 1.7 + dbt-sqlserver       |
| ETL Language   | Python 3.11+ (pandas, pyodbc)      |
| Staging Format | Apache Parquet (Snappy compressed) |
| Data Warehouse | SQL Server 2022                    |
| BI             | Power BI (Import Mode)             |

## Medallion Architecture

```
Bronze (Raw)       → Parquet from SQL Server CDC
    ↓
Silver (Cleansed)  → SQL Server staging tables
    ↓
Gold (Business)    → dbt models → Star Schema DW
```

## Pipeline DAG

**DAG:** `etl_sqlserver_to_dw`
**Schedule:** Daily at 02:00 UTC

```
extract_customers ─┐
extract_products  ─┼─→ load_to_staging → check_data
extract_sales     ─┘                           │
                                               ▼
                          ┌──────────────────────────┐
                          │   No data?  → skip_dbt  │
                          │   New data? → run_dbt   │
                          └──────────────────────────┘
                                               │
                          ┌────────────────────┴─────────────────┐
                          ▼                                       ▼
                   dbt_deps → dbt_models → dbt_tests    (or skip)
                          │
                          └──────────────→ update_watermark
```

## Project Structure

```
.
├── dags/
│   └── etl_pipeline.py          # Airflow DAG
├── dbt/
│   ├── dbt_project.yml          # dbt project config
│   ├── profiles.yml             # dbt connection profiles
│   ├── macros/
│   │   └── scd_type2.sql        # SCD Type 2 macro
│   └── models/
│       ├── staging/
│       │   ├── schema.yml        # Source schema + tests
│       │   ├── stg_customers.sql
│       │   ├── stg_products.sql
│       │   └── stg_sales.sql
│       └── marts/
│           ├── schema.yml        # Mart schema + tests
│           ├── dim_customer.sql  # SCD Type 2
│           ├── dim_product.sql   # SCD Type 2
│           ├── dim_location.sql
│           ├── dim_date.sql      # Calendar dimension
│           └── fact_sales.sql
├── src/
│   ├── config.py                # All connection strings + table list
│   ├── extractor.py             # CDC extraction (watermark-based)
│   └── loader.py                # Parquet → SQL Server staging
├── tests/
│   └── test_transformer.py
├── requirements.txt
└── README.md
```

## Setup

### 1. Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Airflow

```bash
# Standalone Airflow (single-node)
airflow standalone

# Or via Docker
docker compose -f airflow-docker.yml up -d

# Set connection (Airflow UI → Admin → Connections)
# Connection ID: sqlserver_default
# Type: Microsoft SQL Server
# Host: your-sqlserver-host
# Database: source_db
```

### 3. dbt

```bash
# Install dbt with SQL Server adapter
pip install dbt-core dbt-sqlserver

# Set environment variables
export DBT_SQLSERVER_SERVER="localhost"
export DBT_SQLSERVER_DATABASE="staging_db"
export DBT_SQLSERVER_SCHEMA="dw"
export DBT_SQLSERVER_USER="sa"
export DBT_SQLSERVER_PASSWORD="YourPassword"

# Init dbt project (already done — just run deps)
cd dbt && dbt deps

# Test connection
dbt debug
```

### 4. Run locally (without Airflow)

```bash
# Extract from SQL Server → Parquet
python -m src.extractor

# Load Parquet → Staging tables
python -m src.loader

# Run dbt transformations
cd dbt && dbt run --target prod
```

### 5. Trigger DAG manually

```bash
airflow dags trigger etl_sqlserver_to_dw
airflow tasks list etl_sqlserver_to_dw
```

## dbt Tests

```bash
# Run column-level and relationship tests
cd dbt && dbt test

# Generate documentation
dbt docs generate && dbt docs serve
```

## Key Features

- **Incremental CDC** — watermark columns avoid full table scans
- **SCD Type 2** — customer and product dimensions track historical changes
- **Data contracts** — dbt schema.yml tests enforce quality at transformation time
- **Idempotent** — re-runs produce same result; no duplicate data
- **Failure isolation** — each extract task is independent; one table failure doesn't block others
