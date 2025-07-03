from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id='K8SPodOperator_Test',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    description='Run a Kubernetes Pod from Airflow',
    catchup=False,
    tags=['test'],
) as dag:

    run_pod = KubernetesPodOperator(
        task_id="run-echo",
        name="run-echo",
        namespace="airflow",
        image="ghcr.io/sserkanguzel/project-1-1-batch-processing-airflow:2025-07-03-0803",
        is_delete_operator_pod=True,
        cmds=["echo", "Hello from KubernetesPodOperator!"]
    )
