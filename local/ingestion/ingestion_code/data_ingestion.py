import requests
import io
import logging

logger = logging.getLogger(__name__)

def download_parquet_to_memory(url):
    try:
        logger.info(f'Download Start: {url}')
        parquet_response = requests.get(url, stream=True)
        parquet_response.raise_for_status()
        buffer = io.BytesIO()
        for chunk in parquet_response.iter_content(chunk_size=8192):
            buffer.write(chunk)
        logger.info(f'Download Complete: {url}')
        return buffer
    except Exception as e:
        logger.error(f'Error downloading parquet file: {e}')
        raise
