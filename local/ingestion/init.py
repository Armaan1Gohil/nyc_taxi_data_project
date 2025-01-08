from utils import DataProcessor
from utils import MinioClient
from utils import WebScraper
from datetime import datetime
import pandas as pd

def main():
    minio_client = MinioClient()
    if minio_client.check_bucket('nyc-project'):
        print('Bucket Exists')
    else:
        data_processor = DataProcessor()

        print('Creating Bucket...')
        # Create MinIO bucket if it doesn't exist
        bucket_name = 'nyc-project'
        minio_client.create_bucket(bucket_name)
        print('Bucket Created')
        
        print('Creating Dimension Tables...')
        # Path for dim-table files
        dimension_path = 'raw-data/dim-table'

        # Download taxi zone dim table
        taxi_zone_lookup_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
        taxi_zone_buffer = data_processor.download_file(taxi_zone_lookup_url)
        minio_client.upload_file(bucket_name, f'{dimension_path}/taxi_zone_lookup.csv', taxi_zone_buffer)

        # Create rate code dim table
        rate_code_dim = {1: 'Standard rate',
                        2: 'JFK', 
                        3: 'Newark', 
                        4: 'Nassau or Westchester', 
                        5: 'Negotiated fare', 
                        6: 'Group ride'}
        rate_code_df = pd.DataFrame(rate_code_dim, columns=['rate_code_id', 'location'])
        minio_client.upload_file(bucket_name, f'{dimension_path}/rate_code.csv', rate_code_df.to_csv(index = False).encode('utf-8'))

        # Create payment type dim table
        payment_type_code_dim = {1: 'Credit card',
                                2: 'Cash',
                                3: 'No charge',
                                4: 'Dispute',
                                5: 'Unknown',
                                6: 'Voided trip'}
        payment_code_df = pd.DataFrame(payment_type_code_dim, columns=['payment_code_id', 'payment_type'])
        minio_client.upload_file(bucket_name, f'{dimension_path}/payment_code.csv', payment_code_df.to_csv(index = False).encode('utf-8'))
        print('Dimension Tables Created')

        print('Creating Date Track File...')
        # Path for date_track file
        date_tracker_path = 'utils'
        # Create & upload date_track file
        minio_client.upload_file(bucket_name, f'{date_tracker_path}/date_track.jsonl', b'')
        print('Created Date Track File')

        print('Downloading Initial Data Files...')
        # Download Files from year 2020 - latest (Backfill)
        page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
        web_scraper = WebScraper(page_url)
        links = web_scraper.get_links(title_filter = 'Yellow Taxi Trip Records')
        for link in links:
            year, month = link.rsplit('/')[-1].rsplit('_')[-1].split('.')[0].split('-')
            if year >= '2023':
                start_time = datetime.now()
                file_name = f'yellow_tripdata_{year}_{month:02}.parquet'
                parquet_buffer = data_processor.download_file(link)
                parquet_path = f'raw-data/{year}/{month:02}/{file_name}'
                minio_client.upload_file(bucket_name, parquet_path, parquet_buffer)
                end_time = datetime.now()
                execution_time = int((end_time - start_time).total_seconds())
                print(f'Downloaded {file_name} | Execution Time: {execution_time} seconds')
        print('Downloaded Initial Data Files')

if __name__ == "__main__":
    main()