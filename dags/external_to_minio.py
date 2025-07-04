from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import boto3
import os
import io

# Configurations
MINIO_ENDPOINT = "http://minio.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "data"
LOCAL_DIR = "/tmp/airflow_data"

def extract_transform_to_parquet():
    # Step 1: Download CSV
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
    response = requests.get(url)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))

    # Step 2: Add run_date column
    run_date = datetime.today().strftime("%Y-%m-%d")
    df["run_date"] = run_date

    # Step 3: Save Parquet to disk
    os.makedirs(LOCAL_DIR, exist_ok=True)
    file_path = f"{LOCAL_DIR}/people_stats_{run_date}.parquet"
    df.to_parquet(file_path, index=False)

def upload_to_minio():
    # Prepare filenames and keys
    run_date = datetime.today().strftime("%Y-%m-%d")
    local_file = f"{LOCAL_DIR}/people_stats_{run_date}.parquet"
    object_key = f"people_stats/run_date={run_date}/people_stats.parquet"

    # Upload to MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    with open(local_file, "rb") as f:
        s3.upload_fileobj(f, MINIO_BUCKET, object_key)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="ExtractAndUploadToMinIO",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["minio", "parquet", "no-xcom"],
) as dag:

    task1 = PythonOperator(
        task_id="extract_transform_to_parquet",
        python_callable=extract_transform_to_parquet,
    )

    task2 = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )

    task1 >> task2
