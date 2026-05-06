# ETL Data Warehouse

ETL pipeline for extracting data from SQL Server and loading into a PostgreSQL star schema for Power BI reporting.

## Quick Start

```bash
pip install -r requirements.txt

export SQL_SERVER_PASSWORD=your_password
export POSTGRES_PASSWORD=your_password

python -m etl_warehouse.pipeline
```

## Project Structure

```
etl_warehouse/
├── __init__.py
├── extractor.py      # SQL Server data extraction
├── transformer.py    # Data transformation to star schema
├── loader.py         # PostgreSQL data loading
├── pipeline.py       # Main ETL orchestration
├── config.yaml       # Configuration management
├── requirements.txt  # Python dependencies
└── tests/
    └── test_transformer.py
```

## Star Schema

### Dimensions
- `dim_customer` - Customer information with SCD Type 2
- `dim_product` - Product catalog with SCD Type 2
- `dim_time` - Date dimension for time-based analysis
- `dim_location` - Geographic locations with SCD Type 2

### Facts
- `fact_sales` - Sales transactions with foreign keys to all dimensions

## Configuration

Edit `config.yaml` to set database connections:

```yaml
database:
  source:
    server: "localhost"
    port: 1433
    database: "SourceDB"
  warehouse:
    server: "localhost"
    port: 5432
    database: "Warehouse"
```

## Testing

```bash
pytest tests/ -v
```