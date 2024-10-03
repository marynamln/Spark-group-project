import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark import SparkConf

def basic_test_df():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    data = [("Maryna", 19), ("Julia", 22)]
    schema = t.StructType([
        t.StructField("name", t.StringType(), True),
        t.StructField("age", t.IntegerType(), True)])
    df = spark_session.createDataFrame(data, schema)
    return df