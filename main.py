from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfc.basic_df_mindiuk import basic_test_df

def display_test_df():
    spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("task app")
                                 .config(conf=SparkConf())
                                 .getOrCreate())

    df = basic_test_df()
    df.show()

if __name__ == '__main__':
    print('This is the project of Melnyk, Malashchuk and Mindiuk')
    display_test_df()