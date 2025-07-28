from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
import os

PROJECT_ID = "ed-tech-analytics"
BUCKET_NAME = "edtech-bucket-rkang1220"
DATASET_NAME = "edtech_dataset"
TABLE_NAME = "hackers_events"
GCS_FILE_PATH = "hackers_events_2024.csv"  # GCS object path
LOCAL_FILE = "/path/to/local/hackers_events_2024.csv"  # Update this path!


def upload_to_gcs():
    """Upload local CSV to GCS."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_PATH)
    blob.upload_from_filename(LOCAL_FILE)
    print(f"Uploaded {LOCAL_FILE} to gs://{BUCKET_NAME}/{GCS_FILE_PATH}")


default_args = {
    "owner": "airflow",
    "retries": 3,
    "start_date": days_ago(1),
}

with DAG(
    "edtech_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  
    catchup=False,
) as dag:

    # 1. Upload to GCS
    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    # 2. Load from GCS to BigQuery
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[GCS_FILE_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # 3. Run transformations (e.g., create core_users)
    create_core_users_task = BigQueryInsertJobOperator(
        task_id="create_core_users",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `ed-tech-analytics.edtech_dataset.core_users` AS
                    SELECT
                        user_id,
                        MIN(timestamp) AS first_seen,
                        COUNTIF(event_name = 'course_enroll') AS total_enrollments
                    FROM `ed-tech-analytics.edtech_dataset.hackers_events`
                    GROUP BY user_id
                """,
                "useLegacySql": False,
            }
        },
    )

    # 4. Run data validation
    data_validation_task = BigQueryInsertJobOperator(
        task_id="data_validation",
        configuration={
            "query": {
                "query": """
                    SELECT
                        COUNT(*) AS total_events,
                        COUNT(DISTINCT user_id) AS total_users
                    FROM `ed-tech-analytics.edtech_dataset.hackers_events`
                """,
                "useLegacySql": False,
            }
        },
    )

    # Dependencies
    upload_to_gcs_task >> gcs_to_bq_task >> create_core_users_task >> data_validation_task
