from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import boto3
from io import BytesIO

# MinIO credentials & config
MINIO_ENDPOINT = "http://minio.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "data"

def extract_transform_to_parquet(**context):
    # Step 1: Download sample CSV
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
    response = requests.get(url)
    response.raise_for_status()

    df = pd.read_csv(pd.compat.StringIO(response.text))

    # Step 2: Add run_date column
    run_date = datetime.today().strftime("%Y-%m-%d")
    df["run_date"] = run_date

    # Step 3: Convert to Parquet in memory
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Step 4: Push buffer and run_date to XCom
    context['ti'].xcom_push(key='parquet_data', value=buffer.getvalue())
    context['ti'].xcom_push(key='run_date', value=run_date)

def upload_to_minio(**context):
    parquet_data = context['ti'].xcom_pull(key='parquet_data', task_ids='extract_transform_to_parquet')
    run_date = context['ti'].xcom_pull(key='run_date', task_ids='extract_transform_to_parquet')
    object_key = f"people_stats/run_date={run_date}/people_stats.parquet"

    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=parquet_data
    )

with DAG(
    dag_id="ExternaltoMinIO",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["minio", "test"],
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
