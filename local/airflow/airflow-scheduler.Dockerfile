FROM apache/airflow:2.10.4-python3.10

CMD [ "airflow", "scheduler" ]