import logging
from web_scraping import get_latest_parquet_links
from data_ingestion import download_parquet
from data_repartition import repartition_parquet

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../../logs/ingestion.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    num_links = 5
    latest_links = get_latest_parquet_links(page_url, num_links=5)
    base_dir = '../../raw_data'
    logger.info(f'Raw Data Directory: {base_dir}')
    
    output_dir = '../../repartition'
    logger.info(f'Repartition Data Directory: {output_dir}')
    for url in latest_links:
        file_name = url.split('/')[-1]
        year, month = file_name.split('_')[-1].split('.')[0].split('-')

        download_parquet(url, file_name, year, month, base_dir)
        
        no_of_partitions = 4
        repartition_parquet(file_name, year, month, no_of_partitions, base_dir, output_dir)

if __name__ == '__main__':
    main()