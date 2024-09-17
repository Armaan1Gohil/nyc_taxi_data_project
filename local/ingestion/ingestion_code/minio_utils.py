# minio_utils.py
import os
import logging
import boto3
from botocore.client import Config

def configure_minio():
    minio_access_key = os.getenv('MINIO_ROOT_USER')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD')

    s3 = boto3.client(
        's3',
        endpoint_url='http://object-storage:9000',
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version='s3v4'),
    )
    return s3

def create_minio_bucket(s3, bucket_name, logger):
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f'Bucket already exists: {bucket_name}')
    except Exception:
        try:
            s3.create_bucket(Bucket=bucket_name)
            logger.info(f'Bucket created successfully: {bucket_name}')
        except Exception as e:
            logger.error(f'Error creating bucket: {e}')
            raise

def upload_to_minio(buffer, s3, bucket_name, object_name, logger):
    try:
        buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=buffer.getvalue())
        logger.info(f"Successfully uploaded to bucket '{bucket_name}': {object_name}")
        buffer.close()
    except Exception as e:
        logger.error(f'Error uploading to MinIO: {e}')
