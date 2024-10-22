from settings import path_to_data
import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark import SparkConf
def read_title_akas_df(path_to_data):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
    df = spark_session.read.csv(path_to_data)
    return df