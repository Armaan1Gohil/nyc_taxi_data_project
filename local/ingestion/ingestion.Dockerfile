FROM python:3.10-slim

WORKDIR /data_project/ingestion

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "init.py"]
