-- Staging: clean and standardize customer data
{{ config(materialized='view') }}

SELECT
    customer_id                                  AS customer_key,
    customer_name,
    LOWER(TRIM(email))                            AS email,
    REPLACE(phone, '-', '')                      AS phone_clean,
    INITCAP(TRIM(city))                          AS city,
    UPPER(TRIM(state))                           AS state,
    UPPER(TRIM(country))                         AS country,
    created_at,
    updated_at,
    -- SCD Type 2 tracking
    CASE
        WHEN updated_at >= {{ var('watermark_column', "'2000-01-01'") }}
        THEN updated_at
        ELSE CURRENT_TIMESTAMP
    END                                           AS effective_from,
    NULL::timestamp                              AS effective_to,
    TRUE                                         AS is_current
FROM {{ source('staging', 'staging_customers') }}
