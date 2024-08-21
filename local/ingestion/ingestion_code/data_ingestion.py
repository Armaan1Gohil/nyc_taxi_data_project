import requests
import os
import logging

logger = logging.getLogger(__name__)

def download_parquet(url, file_name, year, month, base_dir):
    folder_path = f"{base_dir}/{year}/{month}"
    file_path = f"{folder_path}/{file_name}"
    os.makedirs(folder_path, exist_ok=True)
    logger.info(f'Download Start: {file_name}')

    parquet_response = requests.get(url, stream=True)
    parquet_response.raise_for_status()
    with open(file_path, 'wb') as file:
        for chunk in parquet_response.iter_content(chunk_size=8192):
            file.write(chunk)
    logger.info(f'Download Complete: {file_name}')