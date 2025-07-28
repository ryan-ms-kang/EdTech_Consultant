-- Conversion Analysis for understanding how many users progress through each key product milestone after signup 

-- User Activation Funnel:
-- Step 1: user_signup
-- Step 2: course_search
-- Step 3: course_enroll
-- Step 4: lesson_start
-- Step 5: course_complete

WITH funnel_steps AS (
  SELECT
    user_id,
    MAX(CASE WHEN event_name = 'user_signup' THEN 1 ELSE 0 END) AS did_signup,
    MAX(CASE WHEN event_name = 'course_search' THEN 1 ELSE 0 END) AS did_search,
    MAX(CASE WHEN event_name = 'course_enroll' THEN 1 ELSE 0 END) AS did_enroll,
    MAX(CASE WHEN event_name = 'lesson_start' THEN 1 ELSE 0 END) AS did_lesson,
    MAX(CASE WHEN event_name = 'course_complete' THEN 1 ELSE 0 END) AS did_complete
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  GROUP BY user_id
)

SELECT
  COUNTIF(did_signup = 1) AS total_signups,
  COUNTIF(did_search = 1) AS total_searches,
  COUNTIF(did_enroll = 1) AS total_enrollments,
  COUNTIF(did_lesson = 1) AS total_lessons_started,
  COUNTIF(did_complete = 1) AS total_completions,

  -- Conversion rates
  SAFE_DIVIDE(COUNTIF(did_search = 1), COUNTIF(did_signup = 1)) AS signup_to_search_rate,
  SAFE_DIVIDE(COUNTIF(did_enroll = 1), COUNTIF(did_search = 1)) AS search_to_enroll_rate,
  SAFE_DIVIDE(COUNTIF(did_lesson = 1), COUNTIF(did_enroll = 1)) AS enroll_to_lesson_rate,
  SAFE_DIVIDE(COUNTIF(did_complete = 1), COUNTIF(did_lesson = 1)) AS lesson_to_completion_rate,

  -- Overall conversion: signup → completion
  SAFE_DIVIDE(COUNTIF(did_complete = 1), COUNTIF(did_signup = 1)) AS signup_to_completion_rate
FROM funnel_steps;
