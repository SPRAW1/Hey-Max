


SELECT
DATE_TRUNC(DATE(event_date), WEEK(MONDAY)) AS week_start,
COUNT(DISTINCT events.user_id) AS WAU,
COUNT(DISTINCT (CASE WHEN event_flag = 'primary_action' THEN events.user_id END)) AS core_WAU
FROM {{ref('fct_events')}} events
--{{ref('calendar')}} cal
--LEFT JOIN {{ref('fct_events')}} events
--ON cal.calendar_date = events.event_date
--WHERE cal.calendar_date >= '2025-02-27'
GROUP BY 1
ORDER BY 1
