FROM apache/airflow:2.10.4-python3.10

EXPOSE 8080

CMD [ "airflow", "webserver" ]