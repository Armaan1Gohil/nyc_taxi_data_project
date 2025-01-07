import argparse
from ingestion.utils import DataProcessor
from ingestion.utils import MinioClient


def main(download_year, download_month):
    # Initialize MinIO and DataProcessor
    bucket_name = 'nyc-project'

    # Create MinIO bucket if it doesn't exist
    minio_client = MinioClient()
    
    # Initiate Data Processor
    data_processor = DataProcessor()
    
    # Declare URL and file name
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{download_year}-{download_month:02}.parquet'
    file_name = f'yellow_tripdata_{download_year}_{download_month:02}.parquet'

    # Process and upload data
    parquet_buffer = data_processor.download_file(url)
    parquet_path = f'raw-data/{download_year}/{download_month:02}/{file_name}'
    minio_client.upload_file(bucket_name, parquet_path, parquet_buffer)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    args = parser.parse_args()
    year = args.year
    month = args.month

    main(year, month)