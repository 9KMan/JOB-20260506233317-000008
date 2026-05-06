from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class BaseLoader(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._engine: Engine = None

    def connect(self) -> Engine:
        if self._engine is not None:
            return self._engine

        driver = self.config['driver']
        server = self.config['server']
        port = self.config['port']
        database = self.config['database']
        username = self.config['username']
        password = self.config['password']

        connection_string = (
            f"{driver}://{username}:{password}@{server}:{port}/{database}"
        )
        self._engine = create_engine(connection_string)
        return self._engine

    def disconnect(self) -> None:
        if self._engine:
            self._engine.dispose()
            self._engine = None

    @abstractmethod
    def load_dimension(self, table_name: str, df: pd.DataFrame,
                       mode: str = 'full') -> int:
        pass

    @abstractmethod
    def load_fact(self, table_name: str, df: pd.DataFrame,
                  mode: str = 'upsert') -> int:
        pass


class PostgreSQLLoader(BaseLoader):

    def create_star_schema(self) -> None:
        schema_sql = [
            text("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_key SERIAL PRIMARY KEY,
                customer_id INTEGER UNIQUE,
                name VARCHAR(255),
                email VARCHAR(255),
                region VARCHAR(100),
                created_date DATE,
                is_current BOOLEAN DEFAULT TRUE,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP
            )
            """),
            text("""
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key SERIAL PRIMARY KEY,
                product_id INTEGER UNIQUE,
                name VARCHAR(255),
                category VARCHAR(100),
                brand VARCHAR(100),
                unit_price DECIMAL(10, 2),
                is_current BOOLEAN DEFAULT TRUE,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP
            )
            """),
            text("""
            CREATE TABLE IF NOT EXISTS dim_time (
                time_key SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                day INTEGER,
                month INTEGER,
                quarter INTEGER,
                year INTEGER,
                weekday VARCHAR(20)
            )
            """),
            text("""
            CREATE TABLE IF NOT EXISTS dim_location (
                location_key SERIAL PRIMARY KEY,
                location_id INTEGER UNIQUE,
                city VARCHAR(100),
                state VARCHAR(100),
                country VARCHAR(100),
                is_current BOOLEAN DEFAULT TRUE,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP
            )
            """),
            text("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                sales_key SERIAL PRIMARY KEY,
                customer_key INTEGER REFERENCES dim_customer(customer_key),
                product_key INTEGER REFERENCES dim_product(product_key),
                time_key INTEGER REFERENCES dim_time(time_key),
                location_key INTEGER REFERENCES dim_location(location_key),
                quantity INTEGER,
                revenue DECIMAL(12, 2)
            )
            """)
        ]

        engine = self.connect()
        with engine.connect() as conn:
            for sql in schema_sql:
                conn.execute(sql)
            conn.commit()

    def load_dimension(self, table_name: str, df: pd.DataFrame,
                       mode: str = 'full') -> int:
        engine = self.connect()
        rows_loaded = 0

        with engine.connect() as conn:
            if mode == 'full':
                df.to_sql(table_name, con=engine, if_exists='replace', index=False)
                rows_loaded = len(df)
            elif mode == 'upsert':
                for _, row in df.iterrows():
                    conn.execute(text(f"""
                        INSERT INTO {table_name} ({', '.join(row.index)})
                        VALUES ({', '.join([':' + str(i) for i in range(len(row))])})
                        ON CONFLICT (customer_id if 'customer' in '{table_name}'
                                     ELSE product_id if 'product' in '{table_name}'
                                     ELSE location_id if 'location' in '{table_name}'
                                     ELSE NULL)
                        DO UPDATE SET
                            name = EXCLUDED.name,
                            email = EXCLUDED.email,
                            region = EXCLUDED.region
                    """), row.to_dict())
                    rows_loaded += 1
            conn.commit()

        return rows_loaded

    def load_fact(self, table_name: str, df: pd.DataFrame,
                  mode: str = 'upsert') -> int:
        engine = self.connect()
        rows_loaded = 0

        with engine.connect() as conn:
            if mode == 'upsert':
                df.to_sql(table_name, con=engine, if_exists='append', index=False)
                rows_loaded = len(df)
            conn.commit()

        return rows_loaded
