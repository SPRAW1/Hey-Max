{{ config(materialized='view') }}

SELECT
 *
FROM {{ source('raw', 'raw_events_data') }}