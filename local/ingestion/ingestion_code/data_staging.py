import logging
from minio_client import MinioClient
from data_ingestion import download_parquet_to_memory

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.minio_client = MinioClient()

    def process_and_upload_to_minio(self, url, file_name, year, month, bucket_name):
        try:
            parquet_buffer = download_parquet_to_memory(url)
            logger.info(f'Uploading file {file_name} to Bucket: {bucket_name}')
            object_name = f'raw_data/{year}/{month}/{file_name}'
            self.minio_client.upload_file(parquet_buffer, bucket_name, object_name)
        except Exception as e:
            logger.error(f'Error processing file {file_name}: {e}')

    def upload_local_files(self, directory, bucket_name, s3_folder):
        for file in os.listdir(directory):
            if file.endswith('.csv'):
                local_path = os.path.join(directory, file)
                s3_path = os.path.join(s3_folder, file)
                self.minio_client.upload_local_file(local_path, bucket_name, s3_path)
