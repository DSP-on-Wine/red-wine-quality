FROM apache/airflow:2.8.4
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR $AIRFLOW_HOME