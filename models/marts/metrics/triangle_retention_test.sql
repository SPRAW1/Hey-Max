
/*
First a CTE to identify first ever event and 1 week cut off date

Then sum the miles_earned

bucket into groups

then see retention of them

*/
WITH user_first_week AS(
SELECT
  user_id,
  MIN(event_date) AS first_event_date,
  DATE_ADD(MIN(event_date),interval 7 day) AS first_week_cut_off_date
FROM {{ ref('fct_events') }}
--WHERE DATE_DIFF(event_date, first_event_date, DAY) <= 7
GROUP BY user_id
),


user_first_week_earned AS(
    SELECT
    e.user_id,
    SUM(e.miles_amount) AS first_week_earnings
    FROM {{ref('fct_events')}} e
    LEFT JOIN user_first_week fw
    ON e.user_id = fw.user_id
    AND e.event_date >= fw.first_event_date
    AND e.event_date < fw.first_week_cut_off_date
    WHERE first_event_date IS NOT NULL
    AND event_type = 'miles_earned'
    GROUP BY 1
    ORDER BY 1,2

),

user_earned_buckets AS (
SELECT
    user_id,
    first_week_earnings,
    
    CASE
      WHEN first_week_earnings < 1000 THEN '0–999'
      WHEN first_week_earnings BETWEEN 1000 AND 1499 THEN '1000–1499'
      WHEN first_week_earnings BETWEEN 1500 AND 1999 THEN '1500–1999'
      WHEN first_week_earnings BETWEEN 2000 AND 2999 THEN '2000–2999'
      WHEN first_week_earnings BETWEEN 3000 AND 3999 THEN '3000–3999'
      ELSE '4000+'  -- includes 4000 and above
    END AS earnings_bucket

  FROM user_first_week_earned
),

user_cohort_activity AS(

 SELECT
    e.user_id,
    e.event_date,
    b.earnings_bucket,
    f.first_event_date,
    DATE_DIFF(DATE(e.event_date), f.first_event_date, DAY) AS days_since_first_event
  FROM {{ ref('fct_events') }} e
  LEFT JOIN user_first_week f ON e.user_id = f.user_id
  LEFT JOIN user_earned_buckets b ON e.user_id = b.user_id
),

retention_by_bucket_day AS(
SELECT
    earnings_bucket,
    days_since_first_event,
    COUNT(DISTINCT user_id) AS retained_users
  FROM user_cohort_activity
  GROUP BY 1, 2
  ),

cohort_size AS(
SELECT
    earnings_bucket,
    COUNT(DISTINCT user_id) AS cohort_size
FROM user_earned_buckets
GROUP BY 1
ORDER BY 1 ASC
)

SELECT
  r.earnings_bucket,
  r.days_since_first_event,
  r.retained_users,
  c.cohort_size,
  (r.retained_users)/(c.cohort_size*1.0) AS retention_rate
  FROM retention_by_bucket_day r
  LEFT JOIN cohort_size c
  ON r.earnings_bucket = c.earnings_bucket
  WHERE r.earnings_bucket IS NOT NULL
  ORDER BY r.earnings_bucket, r.days_since_first_event

