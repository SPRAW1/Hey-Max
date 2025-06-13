-- Step 1: Identify each user's cohort (first active date) and their user info
WITH ranked_first_events AS (
  SELECT
    user_id,
    DATE(event_date) AS cohort_date,
    country,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date) AS rn
  FROM {{ ref('fct_events') }}
),

user_cohorts AS (
  SELECT
    user_id,
    cohort_date,
    country
  FROM ranked_first_events
  WHERE rn = 1
),

-- Step 2: Map each user's events to their cohort
user_activity AS (
  SELECT
    e.user_id,
    u.cohort_date,
    DATE(e.event_date) AS activity_date,
    DATE_DIFF(DATE(e.event_date), u.cohort_date, DAY) AS days_since_cohort,
    u.country--,
    --u.platform,
    --u.country
  FROM {{ ref('fct_events') }} e
  LEFT JOIN user_cohorts u ON e.user_id = u.user_id
  AND cohort_date < '2025-06-01'
),

-- Step 3: Count active users per cohort per day
retention_daily AS (
  SELECT
    --cohort_date,
    days_since_cohort,
    country,
    --platform,
    --country,
    COUNT(DISTINCT user_id) AS retained_users
  FROM user_activity
  GROUP BY days_since_cohort, country--, platform, country
),

-- Step 4: Get cohort sizes
cohort_sizes AS (
  SELECT
    --cohort_date,
    country,
    --platform,
    --country,
    COUNT(DISTINCT user_id) AS cohort_size
  FROM user_cohorts
  GROUP BY country--, platform, country
)

-- Step 5: Final output with retention and churn
SELECT
  r.days_since_cohort,
  r.country,
--  r.platform,
--  r.country,
  r.retained_users,
  c.cohort_size,
  SAFE_DIVIDE(r.retained_users, c.cohort_size) AS retention_rate,
  1 - SAFE_DIVIDE(r.retained_users, c.cohort_size) AS churn_rate
FROM retention_daily r
JOIN cohort_sizes c
  ON  r.country = c.country
  --AND r.platform = c.platform
  --AND r.country = c.country
ORDER BY  r.days_since_cohort,r.country
