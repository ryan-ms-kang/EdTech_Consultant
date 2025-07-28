-- User-level aggregate table

CREATE OR REPLACE TABLE `ed-tech-analytics.edtech_dataset.core_users` AS
SELECT
  user_id,
  MIN(timestamp) AS first_activity_date,
  MAX(timestamp) AS last_activity_date,
  COUNTIF(event_name = 'course_view') AS total_course_views,
  COUNTIF(event_name = 'lesson_start') AS total_lessons_started,
  COUNTIF(event_name = 'lesson_complete') AS total_lessons_completed,
  COUNT(*) AS total_events
FROM `ed-tech-analytics.edtech_dataset.hackers_events`
GROUP BY 1;
