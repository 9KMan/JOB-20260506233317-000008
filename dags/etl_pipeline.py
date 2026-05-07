"""
Airflow DAG: ETL SQL Server → Star Schema Data Warehouse
ETL Strategy: Incremental watermark-based CDC → Bronze → Silver → Gold (dbt)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.sqlite.operators.sqlite import SQLiteOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
import logging

# ── Default Config ──────────────────────────────────────────────────────────────
DAG_CONFIG = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

# ── Watermark state ────────────────────────────────────────────────────────────
WATERMARK_FILE = "/tmp/etl_watermark.txt"


def _read_watermark():
    try:
        with open(WATERMARK_FILE) as f:
            return f.read().strip()
    except FileNotFoundError:
        return "2000-01-01T00:00:00"


def _write_watermark(ts):
    with open(WATERMARK_FILE, "w") as f:
        f.write(ts)


# ── Task: Extract from SQL Server ─────────────────────────────────────────────
def extract_customers(**context):
    import pyodbc
    import pandas as pd
    import logging as lg

    lg.basicConfig(level=lg.INFO)
    log = lg.getLogger(__name__)

    last_run = _read_watermark()
    log.info(f"[extract_customers] Extracting since: {last_run}")

    conn_str = Variable.get("SQLSERVER_CONN_STR", "")
    if not conn_str:
        # Fallback: load from Airflow connection
        conn = BaseHook.get_connection("sqlserver_default")
        conn_str = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={conn.host};"
            f"Database={conn.schema};"
            f"UID={conn.login};"
            f"PWD={conn.password};"
        )

    query = f"""
        SELECT customer_id, customer_name, email, phone, city, state, country,
               created_at, updated_at
        FROM customers
        WHERE updated_at > '{last_run}'
        ORDER BY updated_at;
    """

    try:
        with pyodbc.connect(conn_str, timeout=30) as conn:
            df = pd.read_sql(query, conn)
        log.info(f"[extract_customers] Extracted {len(df)} rows")
        path = f"/tmp/staging/customers_{context['run_id']}.parquet"
        df.to_parquet(path, index=False)
        context["ti"].xcom_push(key="customers_path", value=path)
        context["ti"].xcom_push(key="row_count", value=len(df))
    except Exception as e:
        log.error(f"[extract_customers] Extraction failed: {e}")
        raise AirflowFailException(f"Customer extraction failed: {e}")


def extract_products(**context):
    import pyodbc
    import pandas as pd
    import logging as lg

    lg.basicConfig(level=lg.INFO)
    log = lg.getLogger(__name__)

    last_run = _read_watermark()
    conn_str = Variable.get("SQLSERVER_CONN_STR", "")

    query = f"""
        SELECT product_id, product_name, category, subcategory, unit_cost,
               unit_price, supplier, updated_at
        FROM products
        WHERE updated_at > '{last_run}'
        ORDER BY updated_at;
    """

    try:
        with pyodbc.connect(conn_str, timeout=30) as conn:
            df = pd.read_sql(query, conn)
        log.info(f"[extract_products] Extracted {len(df)} rows")
        path = f"/tmp/staging/products_{context['run_id']}.parquet"
        df.to_parquet(path, index=False)
        context["ti"].xcom_push(key="products_path", value=path)
    except Exception as e:
        raise AirflowFailException(f"Product extraction failed: {e}")


def extract_sales(**context):
    import pyodbc
    import pandas as pd
    import logging as lg

    lg.basicConfig(level=lg.INFO)
    log = lg.getLogger(__name__)

    last_run = _read_watermark()
    conn_str = Variable.get("SQLSERVER_CONN_STR", "")

    query = f"""
        SELECT sale_id, customer_id, product_id, location_id, quantity,
               unit_price, total_amount, sale_date, updated_at
        FROM sales
        WHERE updated_at > '{last_run}'
        ORDER BY updated_at;
    """

    try:
        with pyodbc.connect(conn_str, timeout=30) as conn:
            df = pd.read_sql(query, conn)
        log.info(f"[extract_sales] Extracted {len(df)} rows")
        path = f"/tmp/staging/sales_{context['run_id']}.parquet"
        df.to_parquet(path, index=False)
        context["ti"].xcom_push(key="sales_path", value=path)
    except Exception as e:
        raise AirflowFailException(f"Sales extraction failed: {e}")


# ── Task: Load to staging tables ───────────────────────────────────────────────
def load_to_staging(**context):
    import pandas as pd
    from sqlalchemy import create_engine
    import logging as lg

    lg.basicConfig(level=lg.INFO)
    log = lg.getLogger(__name__)

    staging_engine = Variable.get("STAGING_DB_CONN_STR")

    customers_path = context["ti"].xcom_pull(key="customers_path")
    products_path = context["ti"].xcom_pull(key="products_path")
    sales_path = context["ti"].xcom_pull(key="sales_path")

    engine = create_engine(staging_engine)

    for table, path in [
        ("staging_customers", customers_path),
        ("staging_products", products_path),
        ("staging_sales", sales_path),
    ]:
        if path:
            df = pd.read_parquet(path)
            df.to_sql(
                table,
                engine,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
            log.info(f"[load_to_staging] Loaded {len(df)} rows into {table}")

    engine.dispose()


# ── Task: Update watermark ──────────────────────────────────────────────────────
def update_watermark(**context):
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).isoformat()
    _write_watermark(ts)
    return ts


# ── Branch: check if new data ───────────────────────────────────────────────────
def check_data_available(**context):
    customers_count = context["ti"].xcom_pull(key="row_count") or 0
    if customers_count == 0:
        return "skip_dbt_transforms"
    return "run_dbt_models"


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="etl_sqlserver_to_dw",
    description="Incremental ETL: SQL Server → Star Schema DW via dbt",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",  # Daily at 2 AM UTC
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["etl", "dbt", "sqlserver", "data-warehouse"],
) as dag:

    # ── Stage 1: Extract ────────────────────────────────────────────────────────
    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers,
        provide_context=True,
        pool="sqlserver_pool",
    )

    extract_products = PythonOperator(
        task_id="extract_products",
        python_callable=extract_products,
        provide_context=True,
        pool="sqlserver_pool",
    )

    extract_sales = PythonOperator(
        task_id="extract_sales",
        python_callable=extract_sales,
        provide_context=True,
        pool="sqlserver_pool",
    )

    # ── Stage 2: Load to staging ────────────────────────────────────────────────
    load_staging = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
        provide_context=True,
        pool="staging_db_pool",
    )

    # ── Stage 3: Branch ─────────────────────────────────────────────────────────
    check_data = BranchPythonOperator(
        task_id="check_data_available",
        python_callable=check_data_available,
        provide_context=True,
    )

    # ── Stage 4: dbt transformations ────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/dbt && dbt deps --profiles-dir /opt/dbt/profiles",
    )

    dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /opt/dbt && dbt run "
            "--target prod "
            "--profiles-dir /opt/dbt/profiles "
            "--models marts.dim_customer marts.dim_product marts.fact_sales"
        ),
    )

    dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            "cd /opt/dbt && dbt test "
            "--target prod "
            "--profiles-dir /opt/dbt/profiles"
        ),
    )

    # ── Skip path ───────────────────────────────────────────────────────────────
    skip_dbt = BashOperator(
        task_id="skip_dbt_transforms",
        bash_command="echo 'No new data — skipping dbt transformations'",
    )

    # ── Update watermark ────────────────────────────────────────────────────────
    update_wm = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
        provide_context=True,
    )

    # ── Dependencies ────────────────────────────────────────────────────────────
    [extract_customers, extract_products, extract_sales] >> load_staging
    load_staging >> check_data
    check_data >> [dbt_deps >> dbt_models >> dbt_tests] >> update_wm
    check_data >> skip_dbt >> update_wm
