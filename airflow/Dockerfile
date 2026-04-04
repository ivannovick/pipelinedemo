FROM apache/airflow:2.10.4-python3.12

COPY --chown=airflow:root requirements.txt /tmp/airflow-extra-requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/airflow-extra-requirements.txt
