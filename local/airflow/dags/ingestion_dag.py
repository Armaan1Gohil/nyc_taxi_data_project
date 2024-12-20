from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(dag_id='run_script_in_other_container', 
         default_args=default_args, 
         schedule_interval='@daily', 
         catchup=False
) as dag:
    run_script_task = DockerOperator(
        task_id='run_script',
        image='extraction',
        command='python /data_project/ingestion/ingestion_code/main.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )