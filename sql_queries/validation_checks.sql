-- Post ingestion validation checks

-- Identify events where additional_properties JSON is missing required keys ('device', 'browser')
SELECT event_id, additional_properties
FROM `ed-tech-analytics.edtech_dataset.hackers_events`
WHERE
  (JSON_EXTRACT(additional_properties, '$.device') IS NULL
  OR JSON_EXTRACT(additional_properties, '$.browser') IS NULL);

-- Find dates where event count for any event_name is more than 5x average daily count for that event
WITH daily_counts AS (
  SELECT
    event_name,
    DATE(timestamp) AS event_date,
    COUNT(*) AS daily_count
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  GROUP BY 1, 2
),

avg_counts AS (
  SELECT
    event_name,
    AVG(daily_count) AS avg_daily_count
  FROM daily_counts
  GROUP BY 1
)

SELECT d.event_name, d.event_date, d.daily_count, a.avg_daily_count
FROM daily_counts d
JOIN avg_counts a
  ON d.event_name = a.event_name
WHERE d.daily_count > 5 * a.avg_daily_count
ORDER BY 1, 2;

-- Identify users with course_completed event before course_started for the same course
WITH course_events AS (
  SELECT
    user_id,
    course_id,
    MIN(CASE WHEN event_name = 'course_started' THEN timestamp ELSE NULL END) AS started_at,
    MIN(CASE WHEN event_name = 'course_completed' THEN timestamp ELSE NULL END) AS completed_at
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  GROUP BY 1, 2
)

SELECT user_id, course_id, started_at, completed_at
FROM course_events
WHERE
  completed_at IS NOT NULL
  AND (started_at IS NULL OR completed_at < started_at);

-- Identify session_end events without a preceding session_start for the same user in the past 24 hours
WITH sessions AS (
  SELECT
    user_id,
    event_name,
    timestamp,
    LAG(event_name) OVER (PARTITION BY user_id ORDER BY timestamp) AS prev_event,
    LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS prev_timestamp
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  WHERE
    event_name IN ('session_start', 'session_end')
    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
SELECT user_id, timestamp AS session_end_time
FROM sessions
WHERE
  event_name = 'session_end'
  AND (prev_event IS NULL OR prev_event != 'session_start' 
  OR TIMESTAMP_DIFF(timestamp, prev_timestamp, MINUTE) > 180);

-- Check for null event_id in core_events
SELECT COUNT(*) AS null_event_id_count
FROM `ed-tech-analytics.edtech_dataset.core_events`
WHERE event_id IS NULL;

-- Check for null user_id in core_users
SELECT COUNT(*) AS null_user_id_count
FROM `ed-tech-analytics.edtech_dataset.core_users`
WHERE user_id IS NULL;

-- Check user_id for duplicates
SELECT user_id, COUNT(*) AS num_occurences
FROM `ed-tech-analytics.edtech_dataset.core_users`
GROUP BY 1
HAVING COUNT(*) > 1;

-- Check event_id for duplicates
SELECT event_id, COUNT(*) AS num_occurences
FROM `ed-tech-analytics.edtech_dataset.core_events`
GROUP BY 1
HAVING COUNT(*) > 1;

-- Clock sync errors
SELECT COUNT(*)
FROM `ed-tech-analytics.edtech_dataset.core_events`
WHERE 
  event_time > CURRENT_TIMESTAMP()
  OR event_time IS NULL 
  OR event_time < '2000-01-01';

-- Invalid plan types
SELECT DISTINCT plan_type
FROM `ed-tech-analytics.edtech_dataset.core_events`
WHERE plan_type NOT IN ('free_trial', 'basic', 'premium', 'enterprise');

-- Orphaned users for broken joins or incomplete ingestion
SELECT u.user_id 
FROM `ed-tech-analytics.edtech_dataset.core_users` u 
LEFT JOIN `ed-tech-analytics.edtech_dataset.core_events` e
  ON u.user_id = e.user_id
WHERE e.user_id IS NULL;

-- Unexpected event name values
SELECT DISTINCT event_name
FROM `ed-tech-analytics.edtech_dataset.hackers_events`
WHERE event_name NOT IN ('login', 'signup', 'course_started', 'course_completed', 'payment_success', 'payment_failed');

-- Null or invalid user_id
SELECT COUNT(*)
FROM `ed-tech-analytics.edtech_dataset.core_users`
WHERE 
  user_id IS NULL  
  OR user_id = '';


