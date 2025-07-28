-- Event Anomaly Detection
-- Detect days with unusually high event counts (e.g., 3 std deviations above mean)

WITH daily_event_counts AS (
  SELECT
    DATE(timestamp) AS event_date,
    COUNT(*) AS total_events
  FROM `ed-tech-analytics.edtech_dataset.hackers_events`
  GROUP BY event_date
),

stats AS (
  SELECT
    AVG(total_events) AS avg_events,
    STDDEV(total_events) AS stddev_events
  FROM daily_event_counts
)

SELECT
  d.event_date,
  d.total_events,
  s.avg_events,
  s.stddev_events,
  CASE 
    WHEN d.total_events > s.avg_events + 3 * s.stddev_events THEN 'Anomaly'
    ELSE 'Normal'
  END AS anomaly_flag
FROM daily_event_counts d, stats s
ORDER BY 1 DESC;

