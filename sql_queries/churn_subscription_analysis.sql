-- Monthly churn rate (% of users who canceled subscription that month)

WITH monthly_subscriptions AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(timestamp), MONTH) AS month,
    event_name
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  WHERE event_name IN ('subscription_cancel', 'user_signup')
),

active_users AS (
  SELECT
    month,
    COUNT(DISTINCT user_id) AS active_subscribers
  FROM monthly_subscriptions
  WHERE event_name = 'user_signup'
  GROUP BY month
),

churned_users AS (
  SELECT
    month,
    COUNT(DISTINCT user_id) AS churned_subscribers
  FROM monthly_subscriptions
  WHERE event_name = 'subscription_cancel'
  GROUP BY month
)

SELECT
  a.month,
  a.active_subscribers,
  COALESCE(c.churned_subscribers, 0) AS churned_subscribers,
  ROUND(COALESCE(c.churned_subscribers, 0) / a.active_subscribers, 4) AS churn_rate
FROM active_users a
LEFT JOIN churned_users c ON a.month = c.month
ORDER BY a.month;
