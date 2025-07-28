CREATE OR REPLACE TABLE `ed-tech-analytics.edtech_dataset.daily_user_metrics` AS
  WITH base AS (
    SELECT 
      DATE(timestamp) AS event_date,
      user_id
    FROM `ed-tech-analytics.edtech_dataset.hackers_events`
    GROUP BY 1, 2
  ),

  dau AS (
    SELECT 
      event_date,
      COUNT(DISTINCT user_id) AS dau
    FROM base
    GROUP BY 1
  ),

  wau AS (
    SELECT 
      b.event_date,
      COUNT(DISTINCT user_id) AS wau
    FROM base b
    JOIN UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(b.event_date, INTERVAL 6 DAY), b.event_date)) AS rolling_date
      ON DATE(b.event_date) = rolling_date
    GROUP BY 1
  ),

  mau AS (
    SELECT 
      b.event_date,
      COUNT(DISTINCT user_id) AS mau
    FROM base b
    JOIN UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(b.event_date, INTERVAL 29 DAY), b.event_date)) AS rolling_date
    ON DATE(b.event_date) = rolling_date
    GROUP BY 1
  )

  SELECT
    d.event_date AS date,
    d.dau,
    w.wau,
    m.mau,
    CURRENT_TIMESTAMP() AS created_at
  FROM dau d
  JOIN wau w USING (event_date)
  JOIN mau m USING (event_date)
  ORDER BY 1;
