# ETL Data Warehouse: SQL Server to Star Schema

## Project Overview
- **Project Name**: ETL Data Warehouse
- **Type**: ETL Pipeline / Data Engineering
- **Core Functionality**: Extract data from SQL Server operational database, transform into star schema dimensional model, and load into data warehouse for Power BI reporting
- **Target Users**: Data analysts and BI teams using Power BI

## Technical Stack
- Python 3.9+
- SQL Server (source)
- PostgreSQL (target/data warehouse)
- pandas for data transformation
- sqlalchemy for database connectivity
- apache-airflow for orchestration (optional)

## Functionality Specification

### Core Features
1. **Extract Layer**
   - Connect to SQL Server source database
   - Read customer, product, sales, and time data
   - Incremental extraction support with watermark tracking

2. **Transform Layer**
   - Convert normalized SQL Server schema to star schema
   - Dimension tables: dim_customer, dim_product, dim_time, dim_location
   - Fact table: fact_sales
   - Data quality checks and cleansing

3. **Load Layer**
   - Load transformed data into PostgreSQL data warehouse
   - SCD Type 2 support for slowly changing dimensions
   - Fact table incremental load with proper surrogate keys

4. **Configuration**
   - YAML-based configuration management
   - Environment-specific settings (dev/staging/prod)

### Data Model

#### Source (SQL Server - Normalized)
- Customers, Products, Orders, OrderDetails, Dates

#### Target (Star Schema)
```
dim_customer (customer_key, customer_id, name, email, region, created_date)
dim_product (product_key, product_id, name, category, brand, unit_price)
dim_time (time_key, date, day, month, quarter, year, weekday)
dim_location (location_key, location_id, city, state, country)
fact_sales (sales_key, customer_key, product_key, time_key, location_key, quantity, revenue)
```

## Acceptance Criteria
- [ ] ETL pipeline connects to SQL Server source
- [ ] ETL pipeline connects to PostgreSQL warehouse
- [ ] Star schema dimensions created with surrogate keys
- [ ] Fact table properly references dimension keys
- [ ] Configuration managed via YAML
- [ ] Unit tests for core transformation logic
- [ ] Code committed to GitHub repository