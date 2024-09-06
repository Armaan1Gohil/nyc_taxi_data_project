from spark_utils import init_spark
from data_cleaning import clean_data, replace_zero_with_median
from dimension_table import create_datetime_dimension

if __name__ == "__main__":
    # Initialize Spark
    spark, sc = init_spark()

    # Data path
    data_path = '../raw_data/2019/01/*.parquet'
    
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
