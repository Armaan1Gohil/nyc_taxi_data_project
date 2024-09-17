# file_utils.py
import io
import requests
import logging

def download_parquet_to_memory(url, logger):
    file_name = url.split('/')[-1]
    logger.info(f'Download Start: {file_name}')
    
    parquet_response = requests.get(url, stream=True)
    parquet_response.raise_for_status()
    buffer = io.BytesIO()
    
    for chunk in parquet_response.iter_content(chunk_size=8192):
        buffer.write(chunk)

    logger.info(f'Download Complete: {file_name}')
    return buffer

def process_and_upload_to_minio(url, file_name, year, month, s3, bucket_name, logger):
    parquet_buffer = download_parquet_to_memory(url, logger)

    logger.info(f'Uploading file {file_name} to Bucket: {bucket_name}')
    object_name = f'{year}/{month}/{file_name}'

    upload_to_minio(parquet_buffer, s3, bucket_name, object_name, logger)
