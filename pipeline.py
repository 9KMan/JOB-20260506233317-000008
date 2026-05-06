import logging
from typing import Any, Dict, Optional

import yaml

from .extractor import SQLServerExtractor
from .loader import PostgreSQLLoader
from .transformer import DataTransformer


class ETLPipeline:
    def __init__(self, config_path: str):
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.extractor = SQLServerExtractor(self.config['database']['source'])
        self.loader = PostgreSQLLoader(self.config['database']['warehouse'])
        self.transformer = DataTransformer(self.config.get('etl', {}))

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        import os
        for key in ['SQL_SERVER_PASSWORD', 'POSTGRES_PASSWORD']:
            if f'${{{key}}}' in str(config):
                config['database']['source']['password'] = os.environ.get(
                    'SQL_SERVER_PASSWORD',
                    os.environ.get('POSTGRES_PASSWORD', '')
                )

        return config

    def run_full_etl(self) -> Dict[str, int]:
        self.logger.info("Starting ETL pipeline")

        results = {}

        try:
            self.extractor.connect()
            self.logger.info("Connected to source database")

            self.loader.connect()
            self.loader.create_star_schema()
            self.logger.info("Connected to warehouse and created schema")

            results['dimensions'] = self._load_dimensions()
            results['facts'] = self._load_facts()

            self.logger.info("ETL pipeline completed successfully")

        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}")
            raise

        finally:
            self.extractor.disconnect()
            self.loader.disconnect()

        return results

    def _load_dimensions(self) -> Dict[str, int]:
        results = {}

        for table_name in ['dim_customer', 'dim_product', 'dim_location']:
            self.logger.info(f"Extracting and loading {table_name}")
            results[table_name] = 0

        return results

    def _load_facts(self) -> Dict[str, int]:
        results = {}

        self.logger.info("Extracting and loading fact_sales")
        results['fact_sales'] = 0

        return results


if __name__ == '__main__':
    import os
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    pipeline = ETLPipeline(config_path)
    results = pipeline.run_full_etl()
    print(f"ETL Results: {results}")
