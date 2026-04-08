from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

DBT_DIR = "/home/ubuntu/airflow-data-platform/dbt"
DBT_VENV = "/home/ubuntu/dbt-venv"

with DAG(
    dag_id="summary_tables_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "summary", "analytics"],
    description="Generate daily/weekly summary tables such as DAL and completion rates."
) as dag:

    start = EmptyOperator(task_id="start")

    # Build only reporting models (daily_active_learners, course_completion_rates)
    dbt_run_reporting = BashOperator(
        task_id="dbt_run_reporting",
        bash_command=f"""
            source {DBT_VENV}/bin/activate && \
            cd {DBT_DIR} && \
            dbt run --select reporting --profiles-dir ~/.dbt
        """
    )

    dbt_test_reporting = BashOperator(
        task_id="dbt_test_reporting",
        bash_command=f"""
            source {DBT_VENV}/bin/activate && \
            cd {DBT_DIR} && \
            dbt test --select reporting --profiles-dir ~/.dbt
        """
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_run_reporting >> dbt_test_reporting >> end