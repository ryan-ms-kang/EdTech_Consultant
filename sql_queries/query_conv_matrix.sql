CREATE OR REPLACE TABLE `ed-tech-analytics.edtech_dataset.conversion_funnel_metrics` AS
  WITH signups AS (
    SELECT 
      DATE(timestamp) AS event_date,
      COUNT(DISTINCT user_id) AS num_signed_up
    FROM `ed-tech-analytics.edtech_dataset.hackers_events`
    WHERE event_name = 'user_signup'
    GROUP BY 1
  ),

  course_search AS (
    SELECT 
      DATE(timestamp) AS event_date,
      COUNT(DISTINCT user_id) AS num_course_search
    FROM `ed-tech-analytics.edtech_dataset.hackers_events`
    WHERE event_name = 'course_search'
    GROUP BY 1
  ),

  course_enroll AS (
    SELECT 
      DATE(timestamp) AS event_date,
      COUNT(DISTINCT user_id) AS num_course_enroll
    FROM `ed-tech-analytics.edtech_dataset.hackers_events`
    WHERE event_name = 'course_enroll'
    GROUP BY 1
  ),

  lesson_start AS (
    SELECT 
      DATE(timestamp) AS event_date,
      COUNT(DISTINCT user_id) AS num_lesson_start
    FROM `ed-tech-analytics.edtech_dataset.hackers_events`
    WHERE event_name = 'lesson_start'
    GROUP BY 1
  ),

  course_complete AS (
    SELECT 
      DATE(timestamp) AS event_date,
      COUNT(DISTINCT user_id) AS num_course_complete
    FROM `ed-tech-analytics.edtech_dataset.hackers_events`
    WHERE event_name = 'course_complete'
    GROUP BY 1
  )

  SELECT
    s.event_date,
    s.num_signed_up,
    cs.num_course_search,
    ce.num_course_enroll,
    ls.num_lesson_start,
    cc.num_course_complete,
    SAFE_DIVIDE(cs.num_course_search, s.num_signed_up) AS signup_to_search_rate,
    SAFE_DIVIDE(ce.num_course_enroll, cs.num_course_search) AS search_to_enroll_rate,
    SAFE_DIVIDE(ls.num_lesson_start, ce.num_course_enroll) AS enroll_to_start_rate,
    SAFE_DIVIDE(cc.num_course_complete, ls.num_lesson_start) AS start_to_complete_rate,
    CURRENT_TIMESTAMP() AS created_at
  FROM signups s
    LEFT JOIN course_search cs USING (event_date)
    LEFT JOIN course_enroll ce USING (event_date)
    LEFT JOIN lesson_start ls USING (event_date)
    LEFT JOIN course_complete cc USING (event_date)
  ORDER BY 1;
