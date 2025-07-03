FROM apache/airflow:2.7.1-python3.9

# Set the working directory
WORKDIR /opt/airflow


# Install Python requirements
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy DAGs
COPY dags/ /opt/airflow/dags/

COPY airflow_manifests/config/pod_template.yaml /opt/airflow/pod_templates/pod_template.yaml


#COPY plugins/ /opt/airflow/plugins/
