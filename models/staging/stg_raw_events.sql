{{ config(materialized='view') }}

SELECT
 *
 /* user_id,
  event_type,
  event_time,
  country
  */
FROM {{ source('raw', 'raw_events_data') }}