-- Course-level aggregate table

CREATE OR REPLACE TABLE `ed-tech-analytics.edtech_dataset.core_courses` AS 
SELECT 
  course_id,
  COUNTIF(event_name = 'course_view') AS total_course_views,
  COUNTIF(event_name = 'lesson_start') AS total_lessons_started,
  COUNTIF(event_name = 'lesson_complete') AS total_lessons_completed,
  COUNT(DISTINCT user_id) AS unique_users,
  MIN(timestamp) AS first_activity_date,
  MAX(timestamp) AS last_activity_date
FROM `ed-tech-analytics.edtech_dataset.hackers_events`
WHERE course_id IS NOT NULL
GROUP BY 1;
