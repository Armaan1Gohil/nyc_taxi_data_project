from spark_utils import init_spark
from data_cleaning import clean_data, replace_zero_with_median
from dimension_table import create_datetime_dimension
import argparse

if __name__ == "__main__":
    # Initialize Spark
    spark, sc = init_spark()

    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True, help='Year of the data to be processed')
    parser.add_argument('--month', type=int, required=True, help='Month of the data to be processed')

    args = parser.parse_args()

    year = args.year
    maonth = args.month
    catalog = 'nessie'
    namespace = 'nyc_project_db'

    # Data path
    s3_path = 's3a://nyc-project/raw-data/'
    
    # Read data and apply schema
    df = spark.read.option('headers', True).parquet(data_path)
    
    # Clean data
    df = clean_data(df)
    
    # Replace zero passengers with median
    df = replace_zero_with_median(df)
    
    # Create Pickup Dimension Table
    df, pickup_dim = create_datetime_dimension(df, 'pickup_datetime', 'pickup', sc)
    
    # Create Dropoff Dimension Table
    df, dropoff_dim = create_datetime_dimension(df, 'dropoff_datetime', 'dropoff', sc)
    
    # Show the result
    df.show()
