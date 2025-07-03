FROM apache/airflow:2.7.1-python3.9

# Set the working directory
WORKDIR /opt/airflow


# Install Python requirements
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy DAGs
COPY dags/ /opt/airflow/dags/

#COPY plugins/ /opt/airflow/plugins/
