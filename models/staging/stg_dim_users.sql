{{ config(materialized='view') }}

SELECT
user_id,
country
FROM {{ ref('stg_raw_events') }}
GROUP BY 1,2