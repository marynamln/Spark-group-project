import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
if __name__ == '__main__':
    print('This is the project of Melnyk, Malashchuk and Minduk')

spark_session = (SparkSession.builder
                             .master("local")
                             .appName("task app")
                             .config(conf=SparkConf())
                             .getOrCreate())