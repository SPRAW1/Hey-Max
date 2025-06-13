-- Extract only primary user actions from the event stream which are miles_earned or miles_redeemed
WITH events AS (
  SELECT 
    user_id,
    DATE(event_date) AS event_date,
    DATE_TRUNC(DATE(event_date), MONTH) AS month_start
  FROM {{ ref('fct_events') }}
  WHERE event_flag = 'primary_action'
),

-- Get every unique month a user was active
user_monthly_activity AS (
  SELECT DISTINCT user_id, month_start
  FROM events
),

-- For each month a user was active, get the previous active month
user_month_lagged AS (
  SELECT
    user_id,
    month_start AS current_month,
    LAG(month_start) OVER (PARTITION BY user_id ORDER BY month_start) AS prev_month
  FROM user_monthly_activity
),

-- Label user type for the month: new, retained, or resurrected
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
-- Summarize counts
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

-- Calculate churned users as drop from previous month minus retained
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

-- Final output: includes quick ratio for growth health indicator
SELECT 
*,
SAFE_DIVIDE((new_users + resurrected_users),churned_users) AS quick_ratio
FROM final_monthly_metrics
ORDER BY current_month

