from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello World")

with DAG(
    dag_id="PythonOperator_Test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    description='Run a Python Operator from Airflow',
    catchup=False,
    tags=['test'],
) as dag:

    task = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world,
    )