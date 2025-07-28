-- Cohort-based Retention broken down by users who signed up WEEKLY

WITH weekly_signups AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(timestamp), WEEK) AS signup_week
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  WHERE event_name = 'user_signup'
),

user_activity AS (
  SELECT
    user_id,
    DATE(timestamp) AS activity_date
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
)

SELECT
  signup_week,

  -- Day 1 retention
  COUNT(DISTINCT CASE WHEN DATE_DIFF(a.activity_date, signup_week, DAY) = 1 THEN a.user_id END) / 
  COUNT(DISTINCT w.user_id) AS d1_retention,

  -- Day 7 retention
  COUNT(DISTINCT CASE WHEN DATE_DIFF(a.activity_date, signup_week, DAY) = 7 THEN a.user_id END) / 
  COUNT(DISTINCT w.user_id) AS d7_retention,

  -- Day 30 retention
  COUNT(DISTINCT CASE WHEN DATE_DIFF(a.activity_date, signup_week, DAY) = 30 THEN a.user_id END) / 
  COUNT(DISTINCT w.user_id) AS d30_retention,

FROM weekly_signups w
LEFT JOIN user_activity a
  ON w.user_id = a.user_id
GROUP BY 1
ORDER BY 1;

