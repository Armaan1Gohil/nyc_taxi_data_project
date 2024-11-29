import os
import io
import requests
from minio_client import MinioClient

class DataProcessor:

    def download_file(self, url):
        try:
            file_response = requests.get(url, stream=True)
            file_response.raise_for_status()
            buffer = io.BytesIO()
            for chunk in file_response.iter_content(chunk_size=8192):
                buffer.write(chunk)
            buffer.seek(0)
            return buffer.getvalue()
        except Exception as e:
            raise
