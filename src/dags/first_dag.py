from airflow import DAG

from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'orosa',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'first_dag',
    default_args=default_args,
    description='Its my first dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='none',
        bash_command='echo Hala madrid!'
    )

    task1