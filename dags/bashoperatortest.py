from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='BashOperator_Test',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    description='Run a Bash Operator from Airflow',
    catchup=False,
    tags=['test'],
) as dag:
    start_task = BashOperator(
        task_id='start',
        bash_command='echo "Hello from Airflow!"',
    )