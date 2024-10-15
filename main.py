from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfc.basic_df_mindiuk import basic_test_df
from input_output import read_imbd_df

def display_test_df():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

if __name__ == '__main__':
    df = read_imbd_df()
    df.show()