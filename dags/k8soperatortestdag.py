from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="K8SPodOperator_Test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Run a Kubernetes Pod from Airflow using pod_template.yaml",
    tags=["test"],
) as dag:

    run_pod = KubernetesPodOperator(
        task_id="run-echo",
        name="run-echo",
        namespace="airflow",
        pod_template_file="/opt/airflow/pod_templates/pod_template.yaml",
        is_delete_operator_pod=False,
        get_logs=True,
        cmds=["echo", "Hello from KubernetesPodOperator!"]
    )
