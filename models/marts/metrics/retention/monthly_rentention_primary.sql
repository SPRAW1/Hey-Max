WITH events AS (
  SELECT 
    user_id,
    DATE(event_date) AS event_date,
    DATE_TRUNC(DATE(event_date), MONTH) AS month_start
  FROM {{ ref('fct_events') }}
),

user_monthly_activity AS (
  SELECT DISTINCT user_id, month_start
  FROM events
),

user_month_lagged AS (
  SELECT
    user_id,
    month_start AS current_month,
    LAG(month_start) OVER (PARTITION BY user_id ORDER BY month_start) AS prev_month
  FROM user_monthly_activity
),

labeled_users AS (
  SELECT
    current_month,
    user_id,
    CASE
      WHEN prev_month IS NULL THEN 'new'
      WHEN DATE_DIFF(current_month, prev_month, MONTH) = 1 THEN 'retained'
      WHEN DATE_DIFF(current_month, prev_month, MONTH) > 1 THEN 'resurrected'
      ELSE NULL
    END AS user_status
  FROM user_month_lagged
),

monthly_summary AS (
  SELECT
    current_month,
    COUNT(DISTINCT CASE WHEN user_status = 'new' THEN user_id END) AS new_users,
    COUNT(DISTINCT CASE WHEN user_status = 'retained' THEN user_id END) AS retained_users,
    COUNT(DISTINCT CASE WHEN user_status = 'resurrected' THEN user_id END) AS resurrected_users,
    COUNT(DISTINCT user_id) AS monthly_active_users
  FROM labeled_users
  GROUP BY 1
),

final_monthly_metrics AS (
  SELECT
    current_month,
    new_users,
    retained_users,
    resurrected_users,
    monthly_active_users,
    LAG(monthly_active_users) OVER (ORDER BY current_month) - retained_users AS churned_users
  FROM monthly_summary
)

SELECT * FROM final_monthly_metrics
ORDER BY current_month

/*
--Fringe case  as the month has ended early 

--User that has churned in June according to this logic
-- last event date is 31st May 2025

SELECT
user_id,
event_date
FROM {{ref('fct_events')}}
WHERE user_id = 'u_0065'
GROUP BY 1,2
ORDER BY 2
*/