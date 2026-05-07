"""
dbt macro: SCD Type 2 dimension handling
Tracks historical changes in dimension tables
"""

{% macro scd_type2(source_table, key_column, dim_columns, updated_at_column) %}

-- SCD Type 2: insert new version, expire old version
WITH source_data AS (
    SELECT
        {{ key_column }},
        {% for col in dim_columns %}
        {{ col }},
        {% endfor %}
        {{ updated_at_column }} AS effective_from,
        NULL::timestamp AS effective_to,
        TRUE AS is_current
    FROM {{ source_table }}
),

existing_dim AS (
    SELECT
        *,
        LEAD(effective_from) OVER (PARTITION BY {{ key_column }} ORDER BY effective_from) AS next_effective_from
    FROM {{ this }}
    WHERE is_current = TRUE
),

expired_records AS (
    SELECT
        {{ key_column }},
        {% for col in dim_columns %}
        {{ col }},
        {% endfor %}
        effective_from,
        CASE
            WHEN next_effective_from IS NULL THEN NULL::timestamp
            ELSE DATEADD(day, -1, next_effective_from)
        END AS effective_to,
        FALSE AS is_current
    FROM existing_dim
),

changed_records AS (
    SELECT
        s.{{ key_column }},
        {% for col in dim_columns %}
        s.{{ col }},
        {% endfor %}
        s.effective_from,
        s.effective_to,
        s.is_current
    FROM source_data s
    LEFT JOIN existing_dim e
        ON s.{{ key_column }} = e.{{ key_column }}
    WHERE e.{{ key_column }} IS NULL
        OR NOT (
            {% for col in dim_columns %}
            s.{{ col }} = e.{{ col }}{% if not loop.last %} AND {% endif %}
            {% endfor %}
        )
)

SELECT * FROM expired_records
UNION ALL
SELECT * FROM changed_records

{% endmacro %}
