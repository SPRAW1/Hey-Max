SELECT
cal.calendar_date,
COUNT(DISTINCT events.user_id) AS DAU,
COUNT(DISTINCT (CASE WHEN event_flag = 'primary_action' THEN events.user_id END)) AS core_DAU
FROM {{ref('calendar')}} cal
LEFT JOIN {{ref('fct_events')}} events
 ON cal.calendar_date = events.event_date
WHERE cal.calendar_date >= '2025-02-27'
GROUP BY 1
ORDER BY 1
