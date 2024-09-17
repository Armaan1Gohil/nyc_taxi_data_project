# main.py
import logging
from minio_utils import configure_minio, create_minio_bucket, upload_to_minio
from web_scraper import scrape_urls
from file_utils import process_and_upload_to_minio

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/ingestion.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    # Configure MinIO and logging
    s3 = configure_minio()
    logger = setup_logging()

    # Create bucket
    bucket_name = 'data'
    create_minio_bucket(s3, bucket_name, logger)

    # Scrape URLs
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    headers = scrape_urls(page_url)

    # Process and upload files
    data_till_year = 2019
    for link in headers:
        try:
            url = link.get('href').strip()
            file_name = url.split('/')[-1]
            year, month = file_name.split('_')[-1].split('.')[0].split('-')
            if int(year) >= data_till_year:
                process_and_upload_to_minio(url, file_name, year, month, s3, bucket_name, logger)
        except Exception as e:
            logger.error(f'Error processing file {file_name}: {e}')

if __name__ == "__main__":
    main()