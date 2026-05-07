"""
Config: All connection strings and table metadata
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── SQL Server Source ──────────────────────────────────────────────────────────
SQLSERVER_CONN_STR = os.getenv(
    "SQLSERVER_CONN_STR",
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;Database=source_db;UID=sa;PWD=YourPassword;"
)

# ── Staging DW ─────────────────────────────────────────────────────────────────
STAGING_DB_CONN_STR = os.getenv(
    "STAGING_DB_CONN_STR",
    "mssql+pyodbc://sa:YourPassword@localhost:1433/staging_db?driver=ODBC Driver 17 for SQL Server"
)

# ── Source tables to extract ───────────────────────────────────────────────────
SOURCE_TABLES = {
    "customers": {
        "query": """
            SELECT customer_id, customer_name, email, phone, city, state, country,
                   created_at, updated_at
            FROM customers
            WHERE updated_at > '{watermark}'
            ORDER BY updated_at
        """,
        "pk": "customer_id",
        "watermark_col": "updated_at",
    },
    "products": {
        "query": """
            SELECT product_id, product_name, category, subcategory,
                   unit_cost, unit_price, supplier, updated_at
            FROM products
            WHERE updated_at > '{watermark}'
            ORDER BY updated_at
        """,
        "pk": "product_id",
        "watermark_col": "updated_at",
    },
    "sales": {
        "query": """
            SELECT sale_id, customer_id, product_id, location_id, quantity,
                   unit_price, total_amount, sale_date, updated_at
            FROM sales
            WHERE updated_at > '{watermark}'
            ORDER BY updated_at
        """,
        "pk": "sale_id",
        "watermark_col": "updated_at",
    },
    "locations": {
        "query": """
            SELECT location_id, location_name, city, state, country, updated_at
            FROM locations
            WHERE updated_at > '{watermark}'
            ORDER BY updated_at
        """,
        "pk": "location_id",
        "watermark_col": "updated_at",
    },
}

# ── Staging table mapping ───────────────────────────────────────────────────────
STAGING_TABLES = {
    "customers": "staging_customers",
    "products": "staging_products",
    "sales": "staging_sales",
    "locations": "staging_locations",
}

# ── Watermark state file ────────────────────────────────────────────────────────
WATERMARK_FILE = "/tmp/etl_watermark.txt"
STAGING_DIR = "/tmp/staging"
