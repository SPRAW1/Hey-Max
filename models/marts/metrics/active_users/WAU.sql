


SELECT
DATE_TRUNC(DATE(event_date), WEEK(MONDAY)) AS week_start,
COUNT(DISTINCT events.user_id) AS WAU,
COUNT(DISTINCT (CASE WHEN event_flag = 'primary_action' THEN events.user_id END)) AS core_WAU
FROM {{ref('fct_events')}} events
GROUP BY 1
ORDER BY 1
