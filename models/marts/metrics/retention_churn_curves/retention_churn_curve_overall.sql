-- Step 1: Identify each user's cohort (first active date)
WITH user_cohorts AS (
  SELECT
    user_id,
    MIN(DATE(event_date)) AS cohort_date
  FROM {{ ref('fct_events') }}
  GROUP BY user_id
),

-- Step 2: Map each user's events to their cohort
user_activity AS (
  SELECT
    e.user_id,
    u.cohort_date,
    DATE(e.event_date) AS activity_date,
    DATE_DIFF(DATE(e.event_date), u.cohort_date, DAY) AS days_since_cohort
  FROM {{ ref('fct_events') }} e
  LEFT JOIN user_cohorts u ON e.user_id = u.user_id
),

-- Step 3: Count active users by days since cohort (no cohort split)
retention_daily AS (
  SELECT
    days_since_cohort,
    COUNT(DISTINCT user_id) AS retained_users
  FROM user_activity
  GROUP BY days_since_cohort
),

-- Step 4: Get total cohort size
cohort_size AS (
  SELECT COUNT(DISTINCT user_id) AS total_users FROM user_cohorts
)

-- Step 5: Calculate overall retention and churn curves
SELECT
  r.days_since_cohort,
  r.retained_users,
  c.total_users AS cohort_size,
  SAFE_DIVIDE(r.retained_users, c.total_users) AS retention_rate,
  1 - SAFE_DIVIDE(r.retained_users, c.total_users) AS churn_rate
FROM retention_daily r
CROSS JOIN cohort_size c
ORDER BY r.days_since_cohort
