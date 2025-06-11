WITH calendar AS (
  SELECT calendar_date
  FROM {{ ref('calendar') }}
  WHERE calendar_date >= '2025-02-27'
  AND calendar_date < '2025-7-27'
),

events AS (
  SELECT 
  DISTINCT user_id, 
  DATE(event_date) AS event_date
  FROM {{ ref('fct_events') }}
),



user_active_today AS (
  SELECT
    cal.calendar_date,
    e.event_date AS active_current_date,
    e.user_id
  FROM calendar cal
  LEFT JOIN events e
    ON cal.calendar_date = e.event_date
    ORDER BY 1,3 ASC
),

user_active_yesterday AS (
  SELECT
    DATE_SUB(active_current_date, INTERVAL 1 DAY) AS prev_day_of_activity,
    user_id
  FROM user_active_today
),


user_last_active_before_today AS (
  SELECT
    cal.calendar_date,
    e.user_id,
    MAX(e.event_date) AS last_active_date
  FROM calendar cal
  LEFT JOIN events e
    ON e.event_date < cal.calendar_date
  GROUP BY cal.calendar_date, e.user_id
),


combined AS (
  SELECT
  
    cal.calendar_date,
    -- uat.user_id,
    --COALESCE(uat.user_id, uay.user_id) AS user_id,
    COALESCE(uat.user_id, uay.user_id, ulab.user_id) AS user_id,

    CASE WHEN uat.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_active_today,
    CASE WHEN uay.user_id IS NOT NULL THEN 1 ELSE 0 END AS was_active_yesterday,
    
CASE 
  WHEN ulab.last_active_date IS NULL THEN 1  -- No past activity
  ELSE 0
END AS is_first_time_user,

    CASE
      WHEN ulab.last_active_date <= DATE_SUB(cal.calendar_date, INTERVAL 1 DAY)
           AND uay.user_id IS NULL THEN 1
      ELSE 0
    END AS was_inactive_yesterday

  FROM calendar cal
  LEFT JOIN user_active_today uat
  ON cal.calendar_date = uat.calendar_date
  LEFT JOIN user_active_yesterday uay 
  ON uat.active_current_date = uay.prev_day_of_activity AND uat.user_id = uay.user_id
  LEFT JOIN user_last_active_before_today ulab
  ON uat.calendar_date = ulab.calendar_date
  AND ulab.user_id = COALESCE(uat.user_id, uay.user_id)
)




, labeled_users AS (
  SELECT
    calendar_date,
    user_id,
    CASE
      --WHEN is_active_today = 1 AND was_active_yesterday = 0 AND was_inactive_yesterday = 0 THEN 'new'
      WHEN is_first_time_user = 1 THEN 'new'
      --WHEN was_inactive_yesterday = 1 THEN 'churned'
      WHEN is_active_today = 1 AND was_active_yesterday = 1  THEN 'retained'
      WHEN is_active_today = 1 AND was_active_yesterday = 0  THEN 'resurrected'
      --  WHEN is_active_today = 1 AND was_active_yesterday = 0 AND last_active_date IS NOT NULL THEN 'resurrected'
      ELSE NULL
    END AS user_status
  FROM combined
),

summarised AS (SELECT
  calendar_date,
  COUNT(DISTINCT CASE WHEN user_status = 'new' THEN user_id END) AS new_users,
  COUNT(DISTINCT CASE WHEN user_status = 'retained' THEN user_id END) AS retained_users,
  COUNT(DISTINCT CASE WHEN user_status = 'resurrected' THEN user_id END) AS resurrected_users,
  COUNT (DISTINCT user_id) AS current_DAU
  --COUNT(DISTINCT CASE WHEN user_status = 'churned' THEN user_id END) AS churned_users
FROM labeled_users
GROUP BY 1
ORDER BY 1
)


SELECT
calendar_date,
new_users,
retained_users,
resurrected_users,
current_DAU,
--LAG(current_DAU,1) OVER (ORDER BY calendar_date ASC) AS prev_DAU,
LAG(current_DAU,1) OVER (ORDER BY calendar_date ASC) - retained_users AS churned_users,
FROM summarised
ORDER BY calendar_date ASC