from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'entries': 1
}

with DAG(
    dag_id='download_yellow_taxi_file',
    default_args=default_args,
    schedule_interval=datetime.timedelta(minutes=5),
    catchup=False
) as dag:
    
    run_ingestion = BashOperator(
        task_id = 'run_ingestion_script',
        bash_command = 'docker exec extraction python /data_project/ingestion/ingestion_code/main.py --year 2018'
    )