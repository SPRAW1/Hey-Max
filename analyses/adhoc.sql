SELECT
    user_id,
    event_type,
    COUNT(*) AS event_count,
    SUM(miles_amount) AS total_miles,
    COUNT(DISTINCT transaction_category) AS dist_tran_category_count,
    MIN(event_time) AS first_event,
    MAX(event_time) AS last_event
 FROM {{ref('fct_events')}}
 GROUP BY 1,2

SELECT
user_id,
event_date
FROM {{ref('fct_events')}}
WHERE user_id = 'u_0065'
GROUP BY 1,2
ORDER BY 2

WITH days_diff AS (
SELECT
user_id,
event_date,
LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date ASC) AS prev_event_date,
DATE_DIFF(event_date,LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date ASC),DAY) AS diff
FROM {{ref('fct_events')}}
--WHERE user_id = 'u_0000'
GROUP BY 1,2
ORDER BY 2,1 ASC
)

SELECT
*
FROM days_diff
WHERE diff >= 7

 SELECT
 user_id,
 event_date,
 event_type
 FROM {{ref('fct_events')}}
ORDER BY 1,2 ASC

WITH calendar AS (
  SELECT calendar_date
  FROM {{ ref('calendar') }}
  WHERE calendar_date >= '2025-02-27'
),

events AS (
  SELECT DISTINCT
    user_id, 
    DATE(event_date) AS event_date
  FROM {{ ref('fct_events') }}
),

user_active_today AS (
  SELECT
    cal.calendar_date,
    e.user_id
  FROM calendar cal
  LEFT JOIN events e
    ON cal.calendar_date = e.event_date
),

user_active_yesterday AS (
  SELECT
    e.user_id,
    e.event_date AS active_date
  FROM events e
),

user_last_active_before_yesterday AS (
  SELECT
    cal.calendar_date,
    e.user_id,
    MAX(e.event_date) AS last_active_date
  FROM calendar cal
  JOIN events e
    ON e.event_date < DATE_SUB(cal.calendar_date, INTERVAL 1 DAY)
  GROUP BY cal.calendar_date, e.user_id
),

combined AS (
  SELECT
    cal.calendar_date,
    COALESCE(uat.user_id, uay.user_id, ulab.user_id) AS user_id,

    CASE WHEN uat.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_active_today,
    CASE WHEN uay.user_id IS NOT NULL AND uay.active_date = DATE_SUB(cal.calendar_date, INTERVAL 1 DAY) THEN 1 ELSE 0 END AS was_active_yesterday,

    CASE WHEN ulab.last_active_date IS NULL THEN 1 ELSE 0 END AS is_first_time_user,

    CASE
      WHEN ulab.last_active_date IS NOT NULL 
           AND ulab.last_active_date < DATE_SUB(cal.calendar_date, INTERVAL 1 DAY)
           AND (uay.user_id IS NULL OR uay.active_date <> DATE_SUB(cal.calendar_date, INTERVAL 1 DAY))
      THEN 1
      ELSE 0
    END AS was_inactive_yesterday

  FROM calendar cal
  LEFT JOIN user_active_today uat ON cal.calendar_date = uat.calendar_date
  LEFT JOIN user_active_yesterday uay ON uay.user_id = uat.user_id OR uay.user_id = COALESCE(uat.user_id, NULL)
  LEFT JOIN user_last_active_before_yesterday ulab
    ON cal.calendar_date = ulab.calendar_date
    AND ulab.user_id = COALESCE(uat.user_id, uay.user_id)
),

labeled_users AS (
  SELECT
    calendar_date,
    user_id,
    CASE
      WHEN is_first_time_user = 1 THEN 'new'
      WHEN is_active_today = 1 AND was_active_yesterday = 1 THEN 'retained'
      WHEN is_active_today = 1 AND was_active_yesterday = 0 AND was_inactive_yesterday = 1 THEN 'resurrected'
      WHEN is_active_today = 0 AND was_active_yesterday = 1 THEN 'churned'
      ELSE NULL
    END AS user_status
  FROM combined
)

SELECT
  calendar_date,
  COUNT(DISTINCT CASE WHEN user_status = 'new' THEN user_id END) AS new_users,
  COUNT(DISTINCT CASE WHEN user_status = 'retained' THEN user_id END) AS retained_users,
  COUNT(DISTINCT CASE WHEN user_status = 'resurrected' THEN user_id END) AS resurrected_users,
  COUNT(DISTINCT CASE WHEN user_status = 'churned' THEN user_id END) AS churned_users
FROM labeled_users
GROUP BY calendar_date
ORDER BY calendar_date