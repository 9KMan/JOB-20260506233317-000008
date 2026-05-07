-- Dimension: Date (calendar dimension)
-- Generates a full date spine for all reporting periods
{{ config(materialized='table') }}

WITH date_series AS (
    SELECT
        CAST('2020-01-01' AS DATE) + ROW_NUMBER() OVER (ORDER BY number) - 1 AS date_value
    FROM master..spt_values
    WHERE type = 'P'
    AND number < 3650  -- 10 years
)

SELECT
    CAST(date_value AS INT)                  AS date_dim_key,
    date_value,
    YEAR(date_value)                         AS year,
    MONTH(date_value)                        AS month,
    DAY(date_value)                          AS day,
    DATEFROMPARTS(YEAR(date_value), MONTH(date_value), 1) AS month_start,
    DATEADD(month, 1, DATEFROMPARTS(YEAR(date_value), MONTH(date_value), 1)) - 1 AS month_end,
    DATENAME(WEEKDAY, date_value)            AS day_name,
    DATENAME(MONTH, date_value)              AS month_name,
    YEAR(date_value) * 100 + MONTH(date_value) AS year_month,
    CASE WHEN MONTH(date_value) IN (1,2,3) THEN 'Q1'
         WHEN MONTH(date_value) IN (4,5,6) THEN 'Q2'
         WHEN MONTH(date_value) IN (7,8,9) THEN 'Q3'
         ELSE 'Q4' END                        AS quarter,
    CASE WHEN date_value = CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END AS is_today,
    CASE WHEN date_value < CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END AS is_past
FROM date_series
