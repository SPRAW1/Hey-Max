{{ config(materialized='view') }}

SELECT
user_id,
event_time,
event_type,
transaction_category,
miles_amount,
platform,
utm_source
FROM {{ ref('stg_raw_events') }}
ORDER BY 1 ASC ,2 DESC