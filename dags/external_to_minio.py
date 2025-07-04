from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import requests
import boto3
import io
import os

# MinIO config
MINIO_ENDPOINT = "http://minio.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "data"
LOCAL_DIR = "/tmp/airflow_data"

def extract_transform_to_parquet(**context):
    # Get run_date from Airflow execution context
    run_date = context['ds']  # e.g., '2025-07-03'
    filename = f"people_stats_{run_date}.parquet"
    local_path = os.path.join(LOCAL_DIR, filename)

    os.makedirs(LOCAL_DIR, exist_ok=True)

    # Download sample CSV
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
    response = requests.get(url)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))
    df["run_date"] = run_date

    df.to_parquet(local_path, index=False)

def upload_to_minio(**context):
    run_date = context['ds']
    filename = f"people_stats_{run_date}.parquet"
    local_path = os.path.join(LOCAL_DIR, filename)
    object_key = f"people_stats/run_date={run_date}/people_stats.parquet"

    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    with open(local_path, "rb") as f:
        s3.upload_fileobj(f, MINIO_BUCKET, object_key)

with DAG(
    dag_id="ExtractAndUploadToMinIO",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["minio"],
) as dag:

    extract = PythonOperator(
        task_id="extract_transform_to_parquet",
        python_callable=extract_transform_to_parquet,
        provide_context=True,
    )

    upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        provide_context=True,
    )

    extract >> upload
