import logging
import boto3
from botocore.client import Config
import os

logger = logging.getLogger(__name__)

class MinioClient:
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            endpoint_url='http://object-storage:9000',
            aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
            aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
            config=Config(signature_version='s3v4')
        )

    def create_bucket(self, bucket_name):
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            logger.info(f'Bucket already exists: {bucket_name}')
        except:
            try:
                self.s3.create_bucket(Bucket=bucket_name)
                logger.info(f'Bucket created successfully: {bucket_name}')
            except Exception as e:
                logger.error(f'Error creating bucket: {e}')
                raise

    def upload_file(self, buffer, bucket_name, object_name):
        try:
            buffer.seek(0)
            self.s3.put_object(Bucket=bucket_name, Key=object_name, Body=buffer.getvalue())
            logger.info(f"Successfully uploaded to bucket '{bucket_name}': {object_name}")
            buffer.close()
        except Exception as e:
            logger.error(f'Error uploading to MinIO: {e}')

    def upload_local_file(self, local_path, bucket_name, s3_path):
        try:
            self.s3.upload_file(local_path, bucket_name, s3_path)
            logger.info(f'Uploaded file: {local_path} to bucket: {bucket_name}')
        except Exception as e:
            logger.error(f'Error uploading local file to MinIO: {e}')