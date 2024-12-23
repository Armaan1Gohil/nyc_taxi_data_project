from bs4 import BeautifulSoup
import argparse
import requests
from data_staging import DataProcessor
from minio_client import MinioClient
from web_scraper import WebScraper
import os
import pandas as pd

def main(download_year):
    # Initialize MinIO and DataProcessor
    bucket_name = 'nyc-project'
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    # Create MinIO bucket if it doesn't exist
    minio_client = MinioClient()
    minio_client.create_bucket(bucket_name)
    
    # Initiate Data Processor
    data_processor = DataProcessor()

    #Initial Webscraper
    parquet_links = WebScraper(page_url)
    headers = parquet_links.fetch_links(title_filter='Yellow Taxi Trip Records')
    
    # Process and upload data
    for link in headers:
        url = link.get('href').strip()
        file_name = url.split('/')[-1]
        year, month = file_name.split('_')[-1].split('.')[0].split('-')
        if int(year) == download_year:
            parquet_buffer = data_processor.download_file(url)
            parquet_path = f'raw-data/{year}/{month}/{file_name}'
            minio_client.upload_file(bucket_name, parquet_path, parquet_buffer)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    args = parser.parse_args()
    main(args.year)