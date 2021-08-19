"""
# EXAMPLE_DBT_AIRFLOW_DAG
A quick example of a Dbt DAG converted to Airflow.
"""
from datetime import datetime
import os

from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from dbt_dag_parser import DbtDagParser


# ADD YOUR manifest.json FILE HERE
MANIFEST_PATH = os.path.join("../manifest.json")


with DAG(
    catchup=False,
    dag_id="EXAMPLE_DBT_AIRFLOW_DAG",
    description="A Dbt DAG -> Airflow DAG",
    default_args=dict(owner="airflow", start_date=datetime(2020, 1, 1)),
    schedule_interval=None,
    doc_md=__doc__,
) as dag:
    dummy_task = DummyOperator(task_id="dummy_task")

    # Parse the Dbt DAG (via the manifest.json file)
    dag_parser = DbtDagParser(
        manifest_path=MANIFEST_PATH,
        dbt_tags=("refresh_weekly",),
    )

    dbt_run_group = dag_parser.convert_to_airflow_dag(dag=dag)  # type: TaskGroup

    # You can treat a TaskGroup as you would a list of tasks
    dummy_task >> dbt_run_group
