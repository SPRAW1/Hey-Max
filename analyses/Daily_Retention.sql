SELECT
    event_date,
    user_id,
    MIN(event_date) OVER (PARTITION BY user_id) AS first_seen_date,
    LAG(active_flag) OVER (PARTITION BY user_id ORDER BY event_date) AS was_active_yesterday,
    -- Boolean: has this user ever been active before?
    COUNT(*) OVER (PARTITION BY user_id ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) > 0 AS was_active_ever
  FROM {{ ref('fct_events') }}