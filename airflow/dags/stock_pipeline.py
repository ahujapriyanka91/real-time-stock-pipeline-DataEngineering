"""
Step 7: Airflow DAG - runs the batch half of the pipeline on a schedule.

    load_to_snowflake  ->  dbt_build
    (MinIO -> RAW)         (bronze -> silver -> gold, plus tests)

Runs every hour at :05 from 10:05 to 17:05 New York time, Monday-Friday.
The 17:05 run picks up the final quotes after the 16:00 market close.
"""

import sys
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

PROJECT_DIR = "/opt/project"            # repo root, mounted into the container
DBT = "/home/airflow/dbt-venv/bin/dbt"  # dbt lives in its own virtualenv (see Dockerfile)


@dag(
    dag_id="stock_pipeline",
    schedule="5 10-17 * * 1-5",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,           # don't backfill old runs; each run loads everything new anyway
    max_active_runs=1,       # never let two runs load at the same time
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["stocks", "snowflake", "dbt"],
    doc_md=__doc__,
)
def stock_pipeline():

    @task
    def load_to_snowflake():
        sys.path.insert(0, PROJECT_DIR)
        from loader import load

        rows = load()
        print(f"Loaded {rows} new rows")
        return rows

    # Own target/log folders: dbt's parse cache in dbt_stocks/target is written by
    # Windows runs and can't be reused inside this Linux container.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --no-use-colors --target-path /tmp/dbt/target --log-path /tmp/dbt/logs",
        cwd=PROJECT_DIR,
    )

    load_to_snowflake() >> dbt_build


stock_pipeline()
