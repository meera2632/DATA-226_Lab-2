from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import timedelta, datetime
import os

# DBT project directory path inside the container
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"

# Pull Snowflake credentials from Airflow connection
conn = BaseHook.get_connection('snowflake_conn')

# Set env vars for dbt Snowflake profile
dbt_env = {
    "DBT_USER": conn.login,
    "DBT_PASSWORD": conn.password,
    "DBT_ACCOUNT": conn.extra_dejson.get("account", ""),
    "DBT_SCHEMA": conn.schema,
    "DBT_DATABASE": conn.extra_dejson.get("database", ""),
    "DBT_ROLE": conn.extra_dejson.get("role", ""),
    "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse", ""),
    "DBT_TYPE": "snowflake"
}

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='lab2_elt_dbt',
    start_date=datetime(2024, 4, 25),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='ELT DAG to run dbt models and tests for stock price analysis',
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=f"{DBT_BIN} run --select input.stock_price+ output.* --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=dbt_env
    )

    dbt_test = BashOperator(
        task_id='dbt_run_tests',
        bash_command=f"{DBT_BIN} test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=dbt_env
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_run_snapshots',
        bash_command=f"{DBT_BIN} snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=dbt_env
    )

    dbt_run >> dbt_test >> dbt_snapshot
