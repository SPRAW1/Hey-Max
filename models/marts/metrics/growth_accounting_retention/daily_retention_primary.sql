
-- Create a date reference table to ensure continuity in time series (even for missing event days)
WITH calendar AS (
  SELECT calendar_date
  FROM {{ ref('calendar') }}
  WHERE calendar_date BETWEEN '2025-02-27' AND '2025-07-27'
),

-- Extract only primary user actions from the event stream which are miles_earned or miles_redeemed
events AS (
  SELECT 
    user_id,
    DATE(event_date) AS event_date
  FROM {{ ref('fct_events') }}
  WHERE event_flag = 'primary_action'
),

-- Get every unique day a user was active
user_active_days AS (
  SELECT DISTINCT user_id, event_date
  FROM events
),

-- Use LAG to find each user's last active day before a given day
user_lagged_events AS (
  SELECT
    user_id,
    event_date AS current_day,
    LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date) AS prev_active_day
  FROM user_active_days
) ,

-- Label user type for the day: new, retained, or resurrected
labeled_users AS (
  SELECT
    u.current_day AS calendar_date,
    u.user_id,
    CASE
      WHEN u.prev_active_day IS NULL THEN 'new'
      WHEN DATE_DIFF(u.current_day, u.prev_active_day, DAY) = 1 THEN 'retained'
      WHEN DATE_DIFF(u.current_day, u.prev_active_day, DAY) > 1 THEN 'resurrected'
      ELSE NULL
    END AS user_status
  FROM user_lagged_events u
),

-- Summarize counts
daily_summary AS (
  SELECT
    calendar_date,
    COUNT(DISTINCT CASE WHEN user_status = 'new' THEN user_id END) AS new_users,
    COUNT(DISTINCT CASE WHEN user_status = 'retained' THEN user_id END) AS retained_users,
    COUNT(DISTINCT CASE WHEN user_status = 'resurrected' THEN user_id END) AS resurrected_users,
    COUNT(DISTINCT user_id) AS current_dau
  FROM labeled_users
  GROUP BY 1
  ORDER BY 1
),

-- Calculate churned users (DAU drop from previous day - retained)
final_daily_metrics AS (
  SELECT
    calendar_date,
    new_users,
    retained_users,
    resurrected_users,
    current_dau,
    LAG(current_dau, 1) OVER (ORDER BY calendar_date) - retained_users AS churned_users
  FROM daily_summary
  ORDER BY 1
)


-- Final output: includes quick ratio for growth health indicator
SELECT 
*,
SAFE_DIVIDE((new_users + resurrected_users),churned_users) AS quick_ratio
FROM final_daily_metrics
ORDER BY calendar_date