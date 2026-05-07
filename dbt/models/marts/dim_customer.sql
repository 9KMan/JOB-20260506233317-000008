-- Dimension: Customer (SCD Type 2)
-- Tracks historical changes to customer attributes
{{ config(materialized='table') }}

WITH deduplicated AS (
    SELECT
        customer_key,
        customer_name,
        email,
        phone_clean,
        city,
        state,
        country,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_key
            ORDER BY updated_at DESC
        ) AS rn
    FROM {{ ref('stg_customers') }}
),

source_data AS (
    SELECT
        customer_key,
        customer_name,
        email,
        phone_clean,
        city,
        state,
        country,
        created_at,
        updated_at
    FROM deduplicated
    WHERE rn = 1
),

indexed AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY customer_key) AS dim_key
    FROM source_data
)

SELECT
    dim_key,
    customer_key,
    customer_name,
    email,
    phone_clean,
    city,
    state,
    country,
    created_at,
    updated_at,
    -- SCD Type 2 metadata
    updated_at                   AS effective_from,
    NULL::timestamp             AS effective_to,
    TRUE                        AS is_current
FROM indexed
