from google.cloud import storage, bigquery
import os

# -----------------------------
# CONFIG
# -----------------------------
PROJECT_ID = "ed-tech-analytics" # This guy from BigQuery
BUCKET_NAME = "edtech-bucket-rkang1220" # This dude also from BigQuery
DATASET_NAME = "edtech_dataset"
TABLE_NAME = "hackers_events" # How the data is saved on BigQuery
LOCAL_FILE = "/Users/minsoggy/Desktop/edtech_proj/data/hackers_events_2024.csv" # Local path to csv 
BLOB_NAME = "hackers_events_2024.csv"  # Name in GCS

# -----------------------------
# UPLOAD TO GCS
# -----------------------------
def upload_to_gcs(bucket_name: str, source_file_name: str, destination_blob_name: str):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    print(f"Uploading {source_file_name} to gs://{bucket_name}/{destination_blob_name}...")
    blob.upload_from_filename(source_file_name)
    print(f"Upload completed: gs://{bucket_name}/{destination_blob_name}")


# -----------------------------
# LOAD INTO BIGQUERY
# -----------------------------
def load_csv_to_bq(project_id: str, dataset_name: str, table_name: str, gcs_uri: str, schema: list):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition="WRITE_TRUNCATE",  # Overwrites table
    )

    print(f"Loading data from {gcs_uri} into {table_id}...")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # Wait for completion

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_id}.")


# -----------------------------
# TABLE SCHEMA DEFINITION
# -----------------------------
schema = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("plan_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("course_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("additional_properties", "STRING", mode="NULLABLE"),
]


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    # 1. Upload file to GCS
    upload_to_gcs(BUCKET_NAME, LOCAL_FILE, BLOB_NAME)

    # 2. Build the GCS URI
    gcs_path = f"gs://{BUCKET_NAME}/{BLOB_NAME}"

    # 3. Load into BigQuery
    load_csv_to_bq(PROJECT_ID, DATASET_NAME, TABLE_NAME, gcs_path, schema)
