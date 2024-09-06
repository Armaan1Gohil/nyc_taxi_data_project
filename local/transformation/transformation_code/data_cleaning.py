from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col

def clean_data(df):
    df = df.filter((col('fare_amount') > 0) & (col('trip_distance') > 0) & (col('extra') > 0))
    df = df.filter(col('ratecode_id') <= 6)
    df = df.withColumn('congestion_surcharge', F.when(col('congestion_surcharge').isNull(), 0).otherwise(col('congestion_surcharge')))
    df = df.withColumn('airport_fee', F.when(col('airport_fee').isNull(), 0).otherwise(col('airport_fee')))
    return df

def replace_zero_with_median(df):
    window_spec = Window.orderBy('passenger_count')
    df_rn = df.select(['passenger_count']).withColumn('rn', F.row_number().over(window_spec))
    
    total_rows = df.count()
    if total_rows % 2 == 0:
        lower_mid = total_rows // 2
        upper_mid = lower_mid + 1
    else:
        lower_mid = total_rows // 2 + 1
        upper_mid = lower_mid
    
    median_df = df_rn.filter((col('rn') == lower_mid) | (col('rn') == upper_mid))
    median_value = median_df.agg(F.avg(col('passenger_count'))).collect()[0][0]
    
    df = df.withColumn('passenger_count', F.when(col('passenger_count') == 0, median_value).otherwise(col('passenger_count')))
    return df
