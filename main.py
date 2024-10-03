from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfc.basic_df_melnyk import basic_test_df

def main():
    spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("task app")
                                 .config(conf=SparkConf())
                                 .getOrCreate())

if __name__ == "__main__":
    main()

    print('This is the project of Melnyk, Malashchuk and Minduk')

    df = basic_test_df()
    print(df.show())