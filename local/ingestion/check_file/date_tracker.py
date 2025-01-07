import json
import os
from ingestion.ingestion_code.download_file.minio_client import MinioClient

class DateTracker:
    minio_client = MinioClient()

    def __init__(self, file_path):
        self.file_path = file_path

    def get_data(self):
        response = DateTracker.minio_client.get_object(Bucket='nyc-project', Key=f'utils/{self.file_path}')
        content = response.read().decode('utf-8')
        lines = content.splitlines()
        data = [json.loads(line) for line in lines]
        return data
    
    def write_data(self, year, month):
        data = {'year': year, 'month': month}
        with open(self.file_path, 'a') as file:
            json.dump(data, file)
            file.write('\n')