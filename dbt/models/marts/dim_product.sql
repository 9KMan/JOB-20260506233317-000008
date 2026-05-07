-- Dimension: Product (SCD Type 2)
{{ config(materialized='table') }}

WITH deduplicated AS (
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        unit_cost,
        unit_price,
        supplier,
        gross_margin,
        gross_margin_pct,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_key
            ORDER BY updated_at DESC
        ) AS rn
    FROM {{ ref('stg_products') }}
),

source_data AS (
    SELECT * FROM deduplicated WHERE rn = 1
),

indexed AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY product_key) AS dim_key
    FROM source_data
)

SELECT
    dim_key,
    product_key,
    product_name,
    category,
    subcategory,
    unit_cost,
    unit_price,
    supplier,
    gross_margin,
    gross_margin_pct,
    updated_at,
    updated_at                   AS effective_from,
    NULL::timestamp             AS effective_to,
    TRUE                        AS is_current
FROM indexed
