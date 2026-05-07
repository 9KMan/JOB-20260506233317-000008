-- Fact: Sales (grain = one row per sales transaction)
{{ config(materialized='table') }}

SELECT
    s.sale_key,
    c.dim_key                               AS customer_dim_key,
    p.dim_key                               AS product_dim_key,
    l.dim_key                               AS location_dim_key,
    d.date_dim_key,
    s.quantity,
    s.unit_price,
    s.total_amount,
    ROUND(s.quantity * p.unit_cost, 2)     AS total_cost,
    ROUND(s.total_amount - (s.quantity * p.unit_cost), 2) AS gross_profit,
    s.sale_date
FROM {{ ref('stg_sales') }} s
JOIN {{ ref('dim_customer') }} c ON s.customer_key = c.customer_key AND c.is_current = TRUE
JOIN {{ ref('dim_product') }} p ON s.product_key = p.product_key AND p.is_current = TRUE
JOIN {{ ref('dim_location') }} l ON s.location_key = l.location_key AND l.is_current = TRUE
JOIN {{ ref('dim_date') }} d ON d.date_value = CAST(s.sale_date AS DATE)
