from airflow import DAG
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime, time

with DAG(
    dag_id='TriggererSensor_Test',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    description='Run a Triggerer from Airflow',
    catchup=False,
    tags=['test'],
) as dag:
    wait_until = TimeSensor(
        task_id='wait_until_5pm',
        target_time=time(17, 2),
    )
