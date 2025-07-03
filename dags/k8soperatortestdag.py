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
        pod_template_file="/opt/airflow/pod_templates/pod_template.yaml",
        is_delete_operator_pod=True,
        cmds=["echo", "Hello from KubernetesPodOperator!"]
    )
