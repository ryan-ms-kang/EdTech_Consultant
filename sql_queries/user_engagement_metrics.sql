-- User Engagement metrics

WITH weekly_sessions AS (
  SELECT
    user_id,
    DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)) AS week_start,
    COUNT(DISTINCT DATE(timestamp)) AS sessions_in_week
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  WHERE event_name IN ('lesson_start', 'course_enroll', 'course_complete')  -- consider key engagement events
  GROUP BY user_id, week_start
)

SELECT
  week_start,
  ROUND(AVG(sessions_in_week), 2) AS avg_sessions_per_user
FROM weekly_sessions
GROUP BY week_start
ORDER BY week_start;
