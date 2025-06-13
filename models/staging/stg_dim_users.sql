{{ config(materialized='table') }}

SELECT
DISTINCT user_id,
country
FROM {{ ref('stg_raw_events') }}
