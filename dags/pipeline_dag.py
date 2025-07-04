from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner":"airflow",
    "depends_on_past":False,
    "retries":3,
    "retry_delay":timedelta(minutes=5),
    "email_on_failure":True,
    "email":["felipelimasant22@gmail.com"]
}

with DAG(
    "medallion_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025,7,1),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id="bronze",
        bash_command="python /opt/airflow/src/bronze.py"
    )
    t2 = BashOperator(
        task_id="silver",
        bash_command="python /opt/airflow/src/silver.py"
    )
    t3 = BashOperator(
        task_id="gold",
        bash_command="python /opt/airflow/src/gold.py"
    )

    t1 >> t2 >> t3
