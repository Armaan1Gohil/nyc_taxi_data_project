from pyspark.sql import SparkSession

def init_spark(app_name='data_transformation'):
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName(app_name) \
        .getOrCreate()
    sc = spark.sparkContext
    return spark, sc
