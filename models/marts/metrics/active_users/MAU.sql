


SELECT
DATE_TRUNC(DATE(event_date), MONTH) AS month_start,
COUNT(DISTINCT events.user_id) AS MAU,
COUNT(DISTINCT (CASE WHEN event_flag = 'primary_action' THEN events.user_id END)) AS core_MAU
FROM {{ref('fct_events')}} events
GROUP BY 1
ORDER BY 1
