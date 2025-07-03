from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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
        config_file=None,  # Use in-cluster config
        is_delete_operator_pod=True,
        pod_template_file="/opt/airflow/pod_templates/pod_template.yaml",
        volumes=[
            {
                "name": "pod-template",
                "configMap": {
                    "name": "airflow-pod-template",
                    "items": [
                        {"key": "pod_template.yaml", "path": "pod_template.yaml"}
                    ],
                },
            }
        ],
        volume_mounts=[
            {
                "name": "pod-template",
                "mountPath": "/opt/airflow/pod_templates",
                "readOnly": True,
            }
        ],
        cmds=["echo", "Hello from KubernetesPodOperator!"],
    )
