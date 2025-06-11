WITH events AS (
  SELECT 
    user_id,
    DATE(event_date) AS event_date,
    DATE_TRUNC(DATE(event_date), WEEK(MONDAY)) AS week_start
  FROM {{ ref('fct_events') }}
),

-- Get unique user-week pairs
user_weekly_activity AS (
  SELECT DISTINCT user_id, week_start
  FROM events
),

-- Get previous week each user was active
user_week_lagged AS (
  SELECT
    user_id,
    week_start AS current_week,
    LAG(week_start) OVER (PARTITION BY user_id ORDER BY week_start) AS prev_week
  FROM user_weekly_activity
),

-- Label user activity
labeled_users AS (
  SELECT
    current_week,
    user_id,
    CASE
      WHEN prev_week IS NULL THEN 'new'
      WHEN DATE_DIFF(current_week, prev_week, WEEK) = 1 THEN 'retained'
      WHEN DATE_DIFF(current_week, prev_week, WEEK) > 1 THEN 'resurrected'
      ELSE NULL
    END AS user_status
  FROM user_week_lagged
),

-- Aggregate
weekly_summary AS (
  SELECT
    current_week,
    COUNT(DISTINCT CASE WHEN user_status = 'new' THEN user_id END) AS new_users,
    COUNT(DISTINCT CASE WHEN user_status = 'retained' THEN user_id END) AS retained_users,
    COUNT(DISTINCT CASE WHEN user_status = 'resurrected' THEN user_id END) AS resurrected_users,
    COUNT(DISTINCT user_id) AS weekly_active_users
  FROM labeled_users
  GROUP BY 1
),

-- Churn = DAU_prev - retained
final_weekly_metrics AS (
  SELECT
    current_week,
    new_users,
    retained_users,
    resurrected_users,
    weekly_active_users,
    LAG(weekly_active_users) OVER (ORDER BY current_week) - retained_users AS churned_users
  FROM weekly_summary
)

SELECT * FROM final_weekly_metrics
ORDER BY current_week


/*
 -- fringe cases that this logic will not capture

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

*/