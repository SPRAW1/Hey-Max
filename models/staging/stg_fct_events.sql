{{ config(materialized='view') }}

SELECT
user_id,
event_time,
event_type,
CASE 
    WHEN event_type in ('miles_earned','miles_redeemed') THEN 'primary_action'
    WHEN event_type in ('like','share','reward_search') THEN 'secondary_action'
    ELSE 'NA'
END AS event_flag,
transaction_category,
miles_amount,
platform,
utm_source

FROM {{ ref('stg_raw_events') }}
ORDER BY 1 ASC ,2 DESC