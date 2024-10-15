from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfc.basic_df_mindiuk import basic_test_df
from input_output import read_imdb_name_basics_df, write_imdb_name_basics_df_to_csv

if __name__ == '__main__':
    df = read_imdb_name_basics_df()
    write_imdb_name_basics_df_to_csv(df, output_path="data/results/name.csv", num_rows=20, mode="overwrite")