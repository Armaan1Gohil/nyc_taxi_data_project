import boto3
import os
from botocore.client import Config
from botocore.exceptions import ClientError

class MinioClient:
    endpoint = 'http://object-storage:9000'
    access_key = os.getenv('MINIO_ROOT_USER')
    secret_key = os.getenv('MINIO_ROOT_PASSWORD')

    def __init__(self):
        self.endpoint = MinioClient.endpoint
        self.access_key = MinioClient.access_key
        self.secret_key = MinioClient.secret_key
        self.s3 = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version='s3v4')
        )
    
    @classmethod
    def set_credentials(cls, endpoint, access_key, secret_key):
        cls.endpoint = endpoint
        cls.access_key = access_key
        cls.secret_key = secret_key

    def create_bucket(self, bucket_name):
        try:
            self.s3.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.s3.create_bucket(Bucket=bucket_name)
            else:
                raise
    
    def check_bucket(self, bucket_name):
        try:
            self.s3.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise
        return True

    def upload_file(self, bucket_name, object_name, file):
        try:
            self.s3.put_object(Bucket=bucket_name, Key=object_name, Body=file)
        except ClientError as e:
            raise