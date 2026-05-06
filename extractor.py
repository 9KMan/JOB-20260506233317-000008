import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class BaseExtractor(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._engine: Optional[Engine] = None

    def connect(self) -> Engine:
        if self._engine is not None:
            return self._engine

        driver = self.config['driver']
        server = self.config['server']
        port = self.config['port']
        database = self.config['database']
        username = self.config['username']
        password = self.config.get('password', os.environ.get('POSTGRES_PASSWORD', ''))

        if 'mssql' in driver:
            password = os.environ.get('SQL_SERVER_PASSWORD', password)
            connection_string = (
                f"{driver}://{username}:{password}@{server}:{port}/{database}"
                "?trust_cert=yes"
            )
        else:
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
    def extract(self, table_name: str, watermark: Optional[str] = None,
                last_watermark: Optional[Any] = None) -> Iterator[pd.DataFrame]:
        pass

    def get_watermark(self, table_name: str, column: str) -> Optional[Any]:
        query = text(f"SELECT MAX({column}) as watermark FROM {table_name}")
        with self.connect().connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            return row[0] if row else None


class SQLServerExtractor(BaseExtractor):

    def extract(self, table_name: str, watermark: Optional[str] = None,
                last_watermark: Optional[Any] = None) -> Iterator[pd.DataFrame]:
        batch_size = self.config.get('batch_size', 10000)

        query = f"SELECT * FROM {table_name}"
        if watermark and last_watermark is not None:
            query += f" WHERE {watermark} > '{last_watermark}'"

        engine = self.connect()

        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()

            batch = []
            for row in result:
                batch.append(row)
                if len(batch) >= batch_size:
                    yield pd.DataFrame(batch, columns=columns)
                    batch = []

            if batch:
                yield pd.DataFrame(batch, columns=columns)
