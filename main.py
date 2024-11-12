from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfc.basic_df_mindiuk import basic_test_df
from basic_dfc.basic_df_malashchuk import basic_test_df as mal
from input_output import read_title_basics_df, write_title_basics_df_to_csv

if __name__ == '__main__':
    df_title_basics = read_title_basics_df()
    write_title_basics_df_to_csv(df_title_basics, output_path="data/results/title_basics.csv", mode="overwrite")