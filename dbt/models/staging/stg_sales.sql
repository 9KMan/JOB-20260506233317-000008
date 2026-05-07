-- Staging: clean and standardize sales fact data
{{ config(materialized='view') }}

SELECT
    sale_id                                 AS sale_key,
    customer_id                             AS customer_key,
    product_id                              AS product_key,
    location_id                             AS location_key,
    quantity,
    unit_price,
    total_amount,
    sale_date,
    -- Date dimensions for easy filtering in BI
    YEAR(sale_date)                         AS sale_year,
    MONTH(sale_date)                        AS sale_month,
    DAY(sale_date)                          AS sale_day,
    DATEFROMPARTS(YEAR(sale_date), MONTH(sale_date), 1) AS sale_month_start,
    updated_at
FROM {{ source('staging', 'staging_sales') }}
WHERE quantity > 0
  AND total_amount > 0
