from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


class DataTransformer:

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def transform_dim_customer(self, df: pd.DataFrame) -> pd.DataFrame:
        dim_customer = pd.DataFrame()
        dim_customer['customer_key'] = range(1, len(df) + 1)
        dim_customer['customer_id'] = df['customer_id']
        dim_customer['name'] = df['name']
        dim_customer['email'] = df['email']
        dim_customer['region'] = df['region']
        dim_customer['created_date'] = pd.to_datetime(df['created_date'])
        dim_customer['is_current'] = True
        dim_customer['valid_from'] = pd.Timestamp.now()
        dim_customer['valid_to'] = pd.Timestamp.max
        return dim_customer

    def transform_dim_product(self, df: pd.DataFrame) -> pd.DataFrame:
        dim_product = pd.DataFrame()
        dim_product['product_key'] = range(1, len(df) + 1)
        dim_product['product_id'] = df['product_id']
        dim_product['name'] = df['name']
        dim_product['category'] = df['category']
        dim_product['brand'] = df['brand']
        dim_product['unit_price'] = df['unit_price']
        dim_product['is_current'] = True
        dim_product['valid_from'] = pd.Timestamp.now()
        dim_product['valid_to'] = pd.Timestamp.max
        return dim_product

    def transform_dim_time(self, df: pd.DataFrame) -> pd.DataFrame:
        dates = pd.to_datetime(df['order_date']).drop_duplicates().sort_values()
        dim_time = pd.DataFrame({'date': dates})
        dim_time['time_key'] = range(1, len(dim_time) + 1)
        dim_time['day'] = dim_time['date'].dt.day
        dim_time['month'] = dim_time['date'].dt.month
        dim_time['quarter'] = dim_time['date'].dt.quarter
        dim_time['year'] = dim_time['date'].dt.year
        dim_time['weekday'] = dim_time['date'].dt.day_name()
        return dim_time

    def transform_dim_location(self, df: pd.DataFrame) -> pd.DataFrame:
        dim_location = pd.DataFrame()
        dim_location['location_key'] = range(1, len(df) + 1)
        dim_location['location_id'] = df['location_id']
        dim_location['city'] = df['city']
        dim_location['state'] = df['state']
        dim_location['country'] = df['country']
        dim_location['is_current'] = True
        dim_location['valid_from'] = pd.Timestamp.now()
        dim_location['valid_to'] = pd.Timestamp.max
        return dim_location

    def transform_fact_sales(self, sales_df: pd.DataFrame,
                             dim_mappings: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        fact_sales = pd.DataFrame()
        fact_sales['sales_key'] = range(1, len(sales_df) + 1)
        fact_sales['customer_key'] = sales_df['customer_id'].map(
            dim_mappings['customer'].set_index('customer_id')['customer_key']
        )
        fact_sales['product_key'] = sales_df['product_id'].map(
            dim_mappings['product'].set_index('product_id')['product_key']
        )
        fact_sales['time_key'] = pd.to_datetime(sales_df['order_date']).map(
            dim_mappings['time'].set_index('date')['time_key']
        )
        fact_sales['location_key'] = sales_df['location_id'].map(
            dim_mappings['location'].set_index('location_id')['location_key']
        )
        fact_sales['quantity'] = sales_df['quantity']
        fact_sales['revenue'] = sales_df['quantity'] * sales_df['unit_price']
        return fact_sales

    def apply_scd_type2(self, dim_df: pd.DataFrame, new_df: pd.DataFrame,
                        key_column: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        current_df = dim_df[dim_df['is_current'] == True].copy()
        changes = new_df.merge(
            current_df,
            on=key_column,
            suffixes=('_new', '_old'),
            how='left'
        )

        expired_records = []
        updated_records = []

        for _, row in changes.iterrows():
            has_changed = (
                row.get('name_new') != row.get('name_old') or
                row.get('email_new') != row.get('email_old') or
                row.get('region_new') != row.get('region_old')
            ) if 'name_new' in row else True

            if has_changed and pd.notna(row.get(f'{key_column}_old')):
                expired_record = row.copy()
                expired_record['is_current'] = False
                expired_record['valid_to'] = pd.Timestamp.now()
                expired_records.append(expired_record)

                new_record = row.copy()
                new_record['is_current'] = True
                new_record['valid_from'] = pd.Timestamp.now()
                new_record['valid_to'] = pd.Timestamp.max
                updated_records.append(new_record)

        return pd.DataFrame(expired_records), pd.DataFrame(updated_records)
