-- Dimension: Location
{{ config(materialized='table') }}

SELECT
    location_id                              AS location_key,
    location_name,
    city,
    state,
    country,
    updated_at,
    updated_at                               AS effective_from,
    NULL::timestamp                          AS effective_to,
    TRUE                                     AS is_current
FROM {{ source('staging', 'staging_locations') }}
