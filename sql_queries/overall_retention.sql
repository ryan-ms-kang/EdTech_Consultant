-- Overall Retention Rate, regardless of when users signed up

WITH signups AS (
  SELECT 
    user_id,
    DATE(timestamp) AS signup_date
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  WHERE event_name = 'user_signup'
)

SELECT
  signup_date,
  COUNT(DISTINCT s.user_id) AS total_users,

  -- Day 1 retention
  COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(e.timestamp), signup_date, DAY) = 1 THEN e.user_id END) / 
  COUNT(DISTINCT s.user_id) AS d1_retention,

  -- Day 7 retention  
  COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(e.timestamp), signup_date, DAY) = 7 THEN e.user_id END) / 
  COUNT(DISTINCT s.user_id) AS d7_retention,

  -- Day 30 retention  
  COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(e.timestamp), signup_date, DAY) = 30 THEN e.user_id END) / 
  COUNT(DISTINCT s.user_id) AS d30_retention
  
FROM signups s
LEFT JOIN `ed-tech-analytics.edtech_dataset.hackers_events` e
  ON s.user_id = e.user_id
  AND DATE(e.timestamp) > signup_date
GROUP BY signup_date
ORDER BY signup_date;
