-- Time-based funnel conversion query (Weekly/Monthly) to trakc funnel performance over time and see if changes improve conversions

WITH signup_users AS (
  SELECT
    user_id,
    plan_type,
    DATE_TRUNC(DATE(timestamp), WEEK) AS signup_week
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  WHERE event_name = 'user_signup'
),

funnel_flags AS (
  SELECT
    s.user_id,
    s.plan_type,
    signup_week,
    MAX(CASE WHEN event_name = 'course_search' THEN 1 ELSE 0 END) AS did_search,
    MAX(CASE WHEN event_name = 'course_enroll' THEN 1 ELSE 0 END) AS did_enroll,
    MAX(CASE WHEN event_name = 'lesson_start' THEN 1 ELSE 0 END) AS did_lesson,
    MAX(CASE WHEN event_name = 'course_complete' THEN 1 ELSE 0 END) AS did_complete
  FROM signup_users s
  LEFT JOIN `ed-tech-analytics.edtech_dataset.hackers_events` e
    ON s.user_id = e.user_id
  GROUP BY 1, 2, 3
)

SELECT
  signup_week,
  COUNT(DISTINCT user_id) AS total_signups,
  COUNTIF(did_search = 1) AS total_searches,
  COUNTIF(did_enroll = 1) AS total_enrollments,
  COUNTIF(did_lesson = 1) AS total_lessons_started,
  COUNTIF(did_complete = 1) AS total_completions,

  SAFE_DIVIDE(COUNTIF(did_search = 1), COUNT(DISTINCT user_id)) AS signup_to_search_rate,
  SAFE_DIVIDE(COUNTIF(did_enroll = 1), COUNTIF(did_search = 1)) AS search_to_enroll_rate,
  SAFE_DIVIDE(COUNTIF(did_lesson = 1), COUNTIF(did_enroll = 1)) AS enroll_to_lesson_rate,
  SAFE_DIVIDE(COUNTIF(did_complete = 1), COUNTIF(did_lesson = 1)) AS lesson_to_completion_rate,
  SAFE_DIVIDE(COUNTIF(did_complete = 1), COUNT(DISTINCT user_id)) AS signup_to_completion_rate
FROM funnel_flags
GROUP BY signup_week
ORDER BY signup_week DESC;
