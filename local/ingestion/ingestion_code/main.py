from bs4 import BeautifulSoup
import requests
from data_staging import DataProcessor
from minio_client import MinioClient
from web_scraper import WebScraper
import os
import pandas as pd

def main():
    # Initialize MinIO and DataProcessor
    bucket_name = 'nyc-project'
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    data_till_year = 2019

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
        if int(year) >= data_till_year:
            parquet_buffer = data_processor.download_file(url)
            parquet_path = f'raw-data/{year}/{month}/{file_name}'
            minio_client.upload_file(bucket_name, parquet_path, parquet_buffer)
    
    # Dimension Tables Creation
    dimension_path = 'raw-data/dim-table'

    taxi_zone_lookup_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv' # Download taxi zone dim table
    taxi_zone_buffer = data_processor.download_file(taxi_zone_lookup_url)
    minio_client.upload_file(bucket_name, f'{dimension_path}/taxi_zone_lookup.csv', taxi_zone_buffer)

    rate_code_dim = {1: 'Standard rate', # Create rate code dim table
                     2: 'JFK', 
                     3: 'Newark', 
                     4: 'Nassau or Westchester', 
                     5: 'Negotiated fare', 
                     6: 'Group ride'}
    rate_code_df = pd.DataFrame(rate_code_dim, columns=['rate_code_id', 'location'])
    minio_client.upload_file(bucket_name, f'{dimension_path}/rate_code.csv', rate_code_df.to_csv(index = False).encode('utf-8'))

    payment_type_code_dim = {1: 'Credit card', # Create payment type dim table
                             2: 'Cash',
                             3: 'No charge',
                             4: 'Dispute',
                             5: 'Unknown',
                             6: 'Voided trip'}
    payment_code_df = pd.DataFrame(payment_type_code_dim, columns=['payment_code_id', 'payment_type'])
    minio_client.upload_file(bucket_name, f'{dimension_path}/payment_code.csv', payment_code_df.to_csv(index = False).encode('utf-8'))

if __name__ == "__main__":
    main()
