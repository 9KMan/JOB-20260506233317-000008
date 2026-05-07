-- Staging: clean and standardize product data
{{ config(materialized='view') }}

SELECT
    product_id                                 AS product_key,
    product_name,
    category,
    subcategory,
    unit_cost,
    unit_price,
    supplier,
    -- Margin calculation
    ROUND(unit_price - unit_cost, 2)            AS gross_margin,
    ROUND((unit_price - unit_cost) / NULLIF(unit_price, 0) * 100, 2) AS gross_margin_pct,
    updated_at
FROM {{ source('staging', 'staging_products') }}
