-- Standardized Event-level table

CREATE OR REPLACE TABLE `ed-tech-analytics.edtech_dataset.core_events` AS  
SELECT 
    event_id,
    user_id,
    event_name,
    TIMESTAMP(event_timestamp) AS event_time,
    course_id,
    plan_type,
    additional_properties
FROM `ed-tech-analytics.edtech_dataset.events` 
WHERE 
  event_id IS NOT NULL
  AND user_id IS NOT NULL
  AND event_name IS NOT NULL;
