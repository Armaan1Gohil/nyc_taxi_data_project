from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='example_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task_1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello, Airflow!"'
    )