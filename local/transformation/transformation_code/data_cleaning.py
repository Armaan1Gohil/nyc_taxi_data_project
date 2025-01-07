from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col
from pyspark.sql import types

class dataCleaning:

    def __init__(self, df):
        self.df = df

    def new_schema(self, df):
        new_schema = types.StructType([
            types.StructField('VendorID', types.IntegerType(), True), 
            types.StructField('tpep_pickup_datetime', types.TimestampType(), True), 
            types.StructField('tpep_dropoff_datetime', types.TimestampType(), True), 
            types.StructField('passenger_count', types.IntegerType(), True), 
            types.StructField('trip_distance', types.FloatType(), True), 
            types.StructField('RatecodeID', types.IntegerType(), True), 
            types.StructField('store_and_fwd_flag', types.StringType(), True), 
            types.StructField('PULocationID', types.IntegerType(), True), 
            types.StructField('DOLocationID', types.IntegerType(), True), 
            types.StructField('payment_type', types.IntegerType(), True), 
            types.StructField('fare_amount', types.FloatType(), True), 
            types.StructField('extra', types.FloatType(), True), 
            types.StructField('mta_tax', types.FloatType(), True), 
            types.StructField('tip_amount', types.FloatType(), True), 
            types.StructField('tolls_amount', types.FloatType(), True), 
            types.StructField('improvement_surcharge', types.FloatType(), True), 
            types.StructField('total_amount', types.FloatType(), True), 
            types.StructField('congestion_surcharge', types.FloatType(), True), 
            types.StructField('airport_fee', types.IntegerType(), True)])

        old_schema = self.df.schema

        for old_field, new_field in zip(old_schema.fields, new_schema.fields):
            df = self.df.withColumn(new_field.name, col(old_field.name).cast(new_field.dataType))
        
        return df


    def filter_data(self, df):
        df = self.df.filter((col('fare_amount') > 0) & (col('trip_distance') > 0) & (col('extra') > 0))
        df = df.filter(col('ratecode_id') <= 6)
        df = df.withColumn('congestion_surcharge', F.when(col('congestion_surcharge').isNull(), 0).otherwise(col('congestion_surcharge')))
        df = df.withColumn('airport_fee', F.when(col('airport_fee').isNull(), 0).otherwise(col('airport_fee')))
        return df

    def replace_zero_with_median(self, df):
        window_spec = Window.orderBy('passenger_count')
        df_rn = self.df.select(['passenger_count']).withColumn('rn', F.row_number().over(window_spec))
        
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