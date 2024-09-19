from bs4 import BeautifulSoup
import requests
from data_staging import DataProcessor
from utils import setup_logging

logger = setup_logging()

def main():
    # Initialize MinIO and DataProcessor
    bucket_name = 'nyc-project'
    data_processor = DataProcessor()
    
    # Create MinIO bucket if it doesn't exist
    data_processor.minio_client.create_bucket(bucket_name)
    
    # Web scraping
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    html_response = requests.get(page_url)
    html_response.raise_for_status()
    soup = BeautifulSoup(html_response.text, 'html.parser')
    headers = soup.find_all('a', attrs={'title': 'Yellow Taxi Trip Records'})
    
    # Process and upload data
    data_till_year = 2019
    for link in headers:
        try:
            url = link.get('href').strip()
            file_name = url.split('/')[-1]
            year, month = file_name.split('_')[-1].split('.')[0].split('-')
            if int(year) >= data_till_year:
                data_processor.process_and_upload_to_minio(url, file_name, year, month, bucket_name)
        except Exception as e:
            logger.error(f'Error processing file {file_name}: {e}')

    # Upload local files
    directory = './dim_table_data/'
    s3_folder = 'dim_table_data'
    data_processor.upload_local_files(directory, bucket_name, s3_folder)

if __name__ == "__main__":
    main()
