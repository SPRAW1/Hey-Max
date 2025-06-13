{{ config(materialized='table') }}

WITH date_spine AS (
  SELECT
    DATE_ADD('2022-01-01', INTERVAL ROW_NUMBER() OVER () - 1 DAY) AS calendar_date
  FROM UNNEST(GENERATE_ARRAY(1, 4000)) 
)

SELECT
  calendar_date,
  EXTRACT(year FROM calendar_date) AS year,
  EXTRACT(month FROM calendar_date) AS month,
  FORMAT_DATE('%Y-%m', calendar_date) AS year_month,
  EXTRACT(week FROM calendar_date) AS week,
  FORMAT_DATE('%Y-%W', calendar_date) AS year_week,
  EXTRACT(dayofweek FROM calendar_date) AS day_of_week,
  CASE
    WHEN EXTRACT(dayofweek FROM calendar_date) IN (1, 7) THEN true
    ELSE false
  END AS is_weekend
FROM date_spine