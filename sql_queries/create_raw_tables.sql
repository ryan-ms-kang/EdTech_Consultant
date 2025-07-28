--- Storing processed & structured analytics data for query performance and scaling

CREATE TABLE edtech_dataset.users (
  user_id STRING NOT NULL,
  signup_date DATE,
  plan_type STRING,
  country STRING,
  is_active BOOL
);

CREATE TABLE edtech_dataset.events (
  event_id STRING NOT NULL,
  user_id STRING NOT NULL,
  event_name STRING,
  event_timestamp TIMESTAMP,
  plan_type STRING,
  course_id STRING,
  additional_properties STRING
);
