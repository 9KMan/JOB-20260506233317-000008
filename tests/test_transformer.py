import pytest
import pandas as pd
from etl_warehouse.transformer import DataTransformer


class TestDataTransformer:
    @pytest.fixture
    def transformer(self):
        config = {'scd_type': 2}
        return DataTransformer(config)

    @pytest.fixture
    def sample_customer_df(self):
        return pd.DataFrame({
            'customer_id': [1, 2, 3],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
            'region': ['North', 'South', 'East'],
            'created_date': ['2024-01-01', '2024-01-02', '2024-01-03']
        })

    @pytest.fixture
    def sample_product_df(self):
        return pd.DataFrame({
            'product_id': [101, 102, 103],
            'name': ['Widget A', 'Widget B', 'Gadget X'],
            'category': ['Electronics', 'Electronics', 'Home'],
            'brand': ['BrandX', 'BrandX', 'BrandY'],
            'unit_price': [29.99, 49.99, 99.99]
        })

    @pytest.fixture
    def sample_sales_df(self):
        return pd.DataFrame({
            'customer_id': [1, 2, 3],
            'product_id': [101, 102, 103],
            'order_date': ['2024-06-01', '2024-06-02', '2024-06-03'],
            'location_id': [1, 2, 3],
            'quantity': [2, 1, 5],
            'unit_price': [29.99, 49.99, 99.99]
        })

    def test_transform_dim_customer(self, transformer, sample_customer_df):
        result = transformer.transform_dim_customer(sample_customer_df)

        assert len(result) == 3
        assert 'customer_key' in result.columns
        assert 'customer_id' in result.columns
        assert 'name' in result.columns
        assert 'email' in result.columns
        assert result['customer_key'].iloc[0] == 1
        assert result['is_current'].iloc[0] == True

    def test_transform_dim_product(self, transformer, sample_product_df):
        result = transformer.transform_dim_product(sample_product_df)

        assert len(result) == 3
        assert 'product_key' in result.columns
        assert 'product_id' in result.columns
        assert 'name' in result.columns
        assert result['product_key'].iloc[0] == 1

    def test_transform_dim_time(self, transformer, sample_sales_df):
        result = transformer.transform_dim_time(sample_sales_df)

        assert len(result) == 3
        assert 'time_key' in result.columns
        assert 'date' in result.columns
        assert 'day' in result.columns
        assert 'month' in result.columns
        assert 'quarter' in result.columns
        assert 'year' in result.columns

    def test_transform_fact_sales(self, transformer, sample_sales_df):
        dim_mappings = {
            'customer': pd.DataFrame({
                'customer_id': [1, 2, 3],
                'customer_key': [1, 2, 3]
            }),
            'product': pd.DataFrame({
                'product_id': [101, 102, 103],
                'product_key': [1, 2, 3]
            }),
            'time': pd.DataFrame({
                'date': pd.to_datetime(['2024-06-01', '2024-06-02', '2024-06-03']),
                'time_key': [1, 2, 3]
            }),
            'location': pd.DataFrame({
                'location_id': [1, 2, 3],
                'location_key': [1, 2, 3]
            })
        }

        result = transformer.transform_fact_sales(sample_sales_df, dim_mappings)

        assert len(result) == 3
        assert 'sales_key' in result.columns
        assert 'customer_key' in result.columns
        assert 'product_key' in result.columns
        assert 'time_key' in result.columns
        assert 'quantity' in result.columns
        assert 'revenue' in result.columns
