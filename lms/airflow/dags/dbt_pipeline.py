# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator
# from datetime import datetime

# DBT_DIR = "/home/ubuntu/airflow-data-platform/dbt"

# with DAG(
#     dag_id="dbt_pipeline",
#     start_date=datetime(2026, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["dbt", "redshift", "model"],
#     description="Run dbt staging, snapshots, dims, facts, and tests."
# ) as dag:

#     start = EmptyOperator(task_id="start")

#     dbt_snapshot = BashOperator(
#         task_id="dbt_snapshot",
#         bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir ~/.dbt"
#     )

#     dbt_run = BashOperator(
#         task_id="dbt_run",
#         bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir ~/.dbt"
#     )

#     dbt_test = BashOperator(
#         task_id="dbt_test",
#         bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir ~/.dbt"
#     )

#     end = EmptyOperator(task_id="end")

#     start >> dbt_snapshot >> dbt_run >> dbt_test >> end
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

DBT_DIR = "/home/ubuntu/airflow-data-platform/dbt"
DBT_VENV = "/home/ubuntu/dbt-venv"  # <-- NEW DBT VENV

with DAG(
    dag_id="dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "redshift", "model"],
    description="Run dbt staging, snapshots, dims, facts, and tests."
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"""
            source {DBT_VENV}/bin/activate && \
            cd {DBT_DIR} && \
            dbt snapshot --profiles-dir ~/.dbt
        """
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            source {DBT_VENV}/bin/activate && \
            cd {DBT_DIR} && \
            dbt run --profiles-dir ~/.dbt
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            source {DBT_VENV}/bin/activate && \
            cd {DBT_DIR} && \
            dbt test --profiles-dir ~/.dbt
        """
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_snapshot >> dbt_run >> dbt_test >> end