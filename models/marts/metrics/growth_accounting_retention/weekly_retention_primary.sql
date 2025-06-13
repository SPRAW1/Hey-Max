-- Extract only primary user actions from the event stream which are miles_earned or miles_redeemed
WITH events AS (
  SELECT 
    user_id,
    DATE(event_date) AS event_date,
    DATE_TRUNC(DATE(event_date), WEEK(MONDAY)) AS week_start
  FROM {{ ref('fct_events') }}
  WHERE event_flag = 'primary_action'
),

-- Get unique user-week pairs
user_weekly_activity AS (
  SELECT DISTINCT user_id, week_start
  FROM events
),

-- For each week a user was active, get the previous active week
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

-- Calculate churned users as drop from previous week minus retained
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

-- Final output: includes quick ratio for growth health indicator
SELECT 
*,
SAFE_DIVIDE((new_users + resurrected_users),churned_users) AS quick_ratio
 FROM final_weekly_metrics
ORDER BY current_week
