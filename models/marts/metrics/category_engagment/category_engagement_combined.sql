WITH cat_user_base AS (
  SELECT
    event_date,
    user_id,
    transaction_category,
    event_type,
    miles_amount
  FROM {{ ref('fct_events') }}
  WHERE event_type IN ('miles_earned', 'miles_redeemed')
  AND event_date < '2025-06-01'
),

active_users AS (
  SELECT
    DATE_TRUNC(event_date, MONTH) AS event_month,
    COUNT(DISTINCT user_id) AS total_active_users
  FROM cat_user_base
  GROUP BY event_month
),

category_metrics AS (
  SELECT
    DATE_TRUNC(event_date, MONTH) AS event_month,
    transaction_category,
    event_type,
    COUNT(DISTINCT user_id) AS users,
    COUNT(*) AS total_events,
    SUM(miles_amount) AS total_miles
  FROM cat_user_base
  GROUP BY event_month, transaction_category, event_type
),

final_metrics AS (
  SELECT
    cm.event_month,
    CASE 
      WHEN cm.event_type = 'miles_earned' THEN 'earned'
      WHEN cm.event_type = 'miles_redeemed' THEN 'redeemed'
    END AS action_type,
    cm.transaction_category,
    cm.users,
    au.total_active_users,
    SAFE_DIVIDE(cm.users, au.total_active_users) AS engagement_rate,
    cm.total_miles,
    SAFE_DIVIDE(cm.total_miles, cm.users) AS avg_miles_per_user,
    SAFE_DIVIDE(cm.total_miles, cm.total_events) AS avg_miles_per_event

  FROM category_metrics cm
  LEFT JOIN active_users au 
    ON cm.event_month = au.event_month
)

SELECT *
FROM final_metrics
ORDER BY event_month, action_type, transaction_category ASC
