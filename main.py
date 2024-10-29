from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from basic_dfc.basic_df_malashchuk import basic_test_df as mal
from basic_dfc.basic_df_mindiuk import basic_test_df

from input_output import read_imdb_name_basics_df, write_imdb_name_basics_df_to_csv
from name_df_cleaning_and_transformation import (convert_columns_to_snake_case,
                                                 convert_death_year_to_int,
                                                 expand_primary_profession,
                                                 fill_missing_values,
                                                 calculate_age_at_death)

from name_df_dealing_with_nulls_and_dublicates import drop_years_columns, fill_missing_professions

if __name__ == '__main__':
    # write_imdb_name_basics_df_to_csv(df, output_path="data/results/name.csv", num_rows=20, mode="overwrite")

    df = read_imdb_name_basics_df()

    df = convert_columns_to_snake_case(df)
    df = convert_death_year_to_int(df)
    df = drop_years_columns(df)

    df = df.dropDuplicates()