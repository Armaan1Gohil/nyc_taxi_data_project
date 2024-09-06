from pyspark.sql import functions as F
from pyspark.sql import types

def create_datetime_dimension(df, datetime_col, dim_name, sc):
    dim_df = df.select([datetime_col]) \
        .distinct() \
        .withColumn(f'{dim_name}_id', F.monotonically_increasing_id()) \
        .withColumn(f'{dim_name}_hour', F.hour(col(datetime_col))) \
        .withColumn(f'{dim_name}_day', F.dayofmonth(col(datetime_col))) \
        .withColumn(f'{dim_name}_month', F.month(col(datetime_col))) \
        .withColumn(f'{dim_name}_year', F.year(col(datetime_col))) \
        .withColumn(f'{dim_name}_weekday', F.date_format(col(datetime_col), 'EEEE'))

    dim_df = dim_df.select(
        f'{dim_name}_id',
        datetime_col,
        f'{dim_name}_hour',
        f'{dim_name}_day',
        f'{dim_name}_month',
        f'{dim_name}_year',
        f'{dim_name}_weekday'
    )
    
    datetime_id_dict = {row[datetime_col]: row[f'{dim_name}_id'] for row in dim_df.collect()} 
    datetime_id_broadcast = sc.broadcast(datetime_id_dict)

    def get_datetime_id(date_time):
        return datetime_id_broadcast.value.get(date_time, None)
    
    get_datetime_id_udf = F.udf(get_datetime_id, types.IntegerType())
    
    df = df.withColumn(f'{dim_name}_id', get_datetime_id_udf(col(datetime_col)))
    
    return df, dim_df
