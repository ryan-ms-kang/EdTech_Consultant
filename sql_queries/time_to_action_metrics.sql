-- Time-to-Action Metrics (average time between key events)

-- Step 1: Time from Signup → Course Enroll
SELECT
  ROUND(AVG(TIMESTAMP_DIFF(enroll.timestamp, signup.timestamp, HOUR)), 2) AS avg_hours_signup_to_enroll,
  ROUND(MIN(TIMESTAMP_DIFF(enroll.timestamp, signup.timestamp, HOUR)), 2) AS min_hours_signup_to_enroll,
  ROUND(MAX(TIMESTAMP_DIFF(enroll.timestamp, signup.timestamp, HOUR)), 2) AS max_hours_signup_to_enroll
FROM `ed-tech-analytics.edtech_dataset.hackers_events` signup
JOIN `ed-tech-analytics.edtech_dataset.hackers_events` enroll
  ON signup.user_id = enroll.user_id
WHERE signup.event_name = 'user_signup'
  AND enroll.event_name = 'course_enroll'
  AND enroll.timestamp > signup.timestamp;

-- Step 2: Time from Lesson Start → Course Complete
SELECT
  ROUND(AVG(TIMESTAMP_DIFF(complete.timestamp, lesson.timestamp, HOUR)), 2) AS avg_hours_lesson_to_complete,
  ROUND(MIN(TIMESTAMP_DIFF(complete.timestamp, lesson.timestamp, HOUR)), 2) AS min_hours_lesson_to_complete,
  ROUND(MAX(TIMESTAMP_DIFF(complete.timestamp, lesson.timestamp, HOUR)), 2) AS max_hours_lesson_to_complete
FROM `ed-tech-analytics.edtech_dataset.hackers_events` lesson
JOIN `ed-tech-analytics.edtech_dataset.hackers_events` complete
  ON lesson.user_id = complete.user_id
WHERE lesson.event_name = 'lesson_start'
  AND complete.event_name = 'course_complete'
  AND complete.timestamp > lesson.timestamp;

-- Step 3: Median Time Between Events 
SELECT
  APPROX_QUANTILES(TIMESTAMP_DIFF(enroll.timestamp, signup.timestamp, HOUR), 100)[OFFSET(50)] AS median_hours_signup_to_enroll
FROM `ed-tech-analytics.edtech_dataset.hackers_events` signup
JOIN `ed-tech-analytics.edtech_dataset.hackers_events` enroll
  ON signup.user_id = enroll.user_id
WHERE signup.event_name = 'user_signup'
  AND enroll.event_name = 'course_enroll'
  AND enroll.timestamp > signup.timestamp;