from ingestion.utils import DataProcessor
from ingestion.utils import MinioClient
import pandas as pd

def main():
    minio_client = MinioClient()
    data_processor = DataProcessor()

    # Create MinIO bucket if it doesn't exist
    bucket_name = 'nyc-project'
    minio_client.create_bucket(bucket_name)
                               

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