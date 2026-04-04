"""
Airflow: Google Drive -> RabbitMQ Stream (binary job envelope).
Downstream: stream-processor container reads the stream, then parses and writes Postgres + disk.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime as pendulum_datetime

from gdrive_stream_publisher import run_publish_pdfs_to_stream

with DAG(
    dag_id="gdrive_publish_to_stream",
    default_args={"owner": "airflow"},
    schedule=timedelta(days=1),
    start_date=pendulum_datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dlt", "rabbitmq-stream", "google-drive"],
) as dag:
    PythonOperator(
        task_id="publish_pdf_jobs_to_stream",
        python_callable=run_publish_pdfs_to_stream,
    )
