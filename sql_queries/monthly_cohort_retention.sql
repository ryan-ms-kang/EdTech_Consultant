WITH signups AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(timestamp), MONTH) AS signup_month
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  WHERE event_name = 'user_signup'
),

user_activity AS (
  SELECT
    e.user_id,
    DATE(e.timestamp) AS activity_date
  FROM `ed-tech-analytics.edtech_dataset.hackers_events` e
),

retention AS (
  SELECT
    s.signup_month,
    COUNT(DISTINCT s.user_id) AS cohort_users,

    -- Day 1 retention
    COUNT(DISTINCT CASE 
      WHEN DATE_DIFF(a.activity_date, s.signup_month, DAY) = 1 
      THEN a.user_id 
    END) AS retained_d1,

    -- Day 7 retention
    COUNT(DISTINCT CASE 
      WHEN DATE_DIFF(a.activity_date, s.signup_month, DAY) = 7 
      THEN a.user_id 
    END) AS retained_d7,

    -- Day 30 retention
    COUNT(DISTINCT CASE 
      WHEN DATE_DIFF(a.activity_date, s.signup_month, DAY) = 30 
      THEN a.user_id 
    END) AS retained_d30

  FROM signups s
  LEFT JOIN user_activity a
    ON s.user_id = a.user_id
  GROUP BY 1
)

SELECT
  signup_month,
  retained_d1 / cohort_users AS d1_retention,
  retained_d7 / cohort_users AS d7_retention,
  retained_d30 / cohort_users AS d30_retention
FROM retention
ORDER BY 1
