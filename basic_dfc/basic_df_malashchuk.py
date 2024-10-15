import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType


def basic_test_df():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    data = [("Victoriia", 19, "Lutsk"), ("Oleksiy", 23, "Boryspil")]
    schema = t.StructType([
        t.StructField("name", t.StringType(), True),
        t.StructField("age", t.IntegerType(), True),
        t.StructField("city", t.StringType(), True)])
    df = spark_session.createDataFrame(data, schema)
    return df