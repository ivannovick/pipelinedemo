"""
Transforms in Postgres (dbt): staging views, marts, FTS — separate from stream ingest.

Runs on its own schedule; tune DBT_RUN_SCHEDULE_MINUTES in the Airflow environment if needed.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime as pendulum_datetime

_dbt_mins = max(1, int(os.environ.get("DBT_RUN_SCHEDULE_MINUTES", "60")))

with DAG(
    dag_id="dbt_run_pdf_archive",
    default_args={"owner": "airflow"},
    schedule=timedelta(minutes=_dbt_mins),
    start_date=pendulum_datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "postgres", "pdf"],
) as dag:
    BashOperator(
        task_id="dbt_run",
        bash_command="""
set -euo pipefail
mkdir -p /tmp/dbt-target /tmp/dbt-logs
cd /opt/dbt/pdf_archive
dbt run --project-dir /opt/dbt/pdf_archive --profiles-dir /opt/dbt/pdf_archive \\
  --target-path /tmp/dbt-target --log-path /tmp/dbt-logs
""".strip(),
    )
