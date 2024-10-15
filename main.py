from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfc.basic_df_mindiuk import basic_test_df
from input_output import read_imdb_name_basics_df

if __name__ == '__main__':
    df = read_imdb_name_basics_df()