from pyspark import SparkConf
from pyspark.sql import SparkSession
from settings import path_to_data
from basic_dfc.basic_df_malashchuk import basic_test_df as mal

from basic_dfc.basic_df_mindiuk import basic_test_df
from input_output import read_title_akas_df



if __name__ == '__main__':
    df2 = read_title_akas_df(path_to_data)
    df2.show()