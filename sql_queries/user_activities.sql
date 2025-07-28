-- DAU
SELECT 
  DATE(timestamp),
  COUNT(DISTINCT user_id) AS dau
FROM `ed-tech-analytics.edtech_dataset.hackers_events` 
GROUP BY 1;

-- WAU
SELECT 
  DATE_TRUNC(timestamp, WEEK(MONDAY)),
  COUNT(DISTINCT user_id) AS wau
FROM `ed-tech-analytics.edtech_dataset.hackers_events` 
GROUP BY 1;

-- MAU
SELECT 
  DATE_TRUNC(timestamp, MONTH),
  COUNT(DISTINCT user_id) AS dau
FROM `ed-tech-analytics.edtech_dataset.hackers_events` 
GROUP BY 1;

-- Retention Rate
WITH signups AS (
  SELECT 
    DISTINCT user_id, 
    DATE(timestamp) AS signup_date
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  WHERE event_name = 'user_signup'
),

returns AS (
  SELECT 
    DISTINCT s.user_id,
    DATE_DIFF(DATE(e.timestamp), s.signup_date, DAY) AS days_after_signup
  FROM signups s
  LEFT JOIN `ed-tech-analytics.edtech_dataset.hackers_events` e
    ON s.user_id = e.user_id
    AND s.signup_date < DATE(e.timestamp)
)

SELECT
  COUNT(DISTINCT IF(days_after_signup = 1, user_id, NULL)) / COUNT(DISTINCT user_id) AS d1_retention,
  COUNT(DISTINCT IF(days_after_signup = 7, user_id, NULL)) / COUNT(DISTINCT user_id) AS d7_retention,
  COUNT(DISTINCT IF(days_after_signup = 30, user_id, NULL)) / COUNT(DISTINCT user_id) AS d30_retention
FROM returns;