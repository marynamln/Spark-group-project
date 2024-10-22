from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfc.basic_df_malashchuk import basic_test_df as mal
from basic_dfc.basic_df_mindiuk import basic_test_df

from input_output import read_imdb_name_basics_df, write_imdb_name_basics_df_to_csv
from name_df_cleaning_and_transformation import (convert_columns_to_snake_case,
                                                 convert_death_year_to_int,
                                                 fill_missing_values)

if __name__ == '__main__':
    df = read_imdb_name_basics_df()
    #write_imdb_name_basics_df_to_csv(df, output_path="data/results/name.csv", num_rows=20, mode="overwrite")

    renamed_col_df = convert_columns_to_snake_case(df)
    transformed_dy_col_df = convert_death_year_to_int(renamed_col_df)
    fill_null_value_df = fill_missing_values(transformed_dy_col_df)
    fill_null_value_df.show(60)

    fill_null_value_df.filter(fill_null_value_df["death_year"].isNull()).show(50)
    fill_null_value_df.filter(fill_null_value_df["birth_year"].isNull()).show(50)
