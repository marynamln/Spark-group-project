from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from basic_dfc.basic_df_malashchuk import basic_test_df as mal
from basic_dfc.basic_df_mindiuk import basic_test_df

from input_output import read_imdb_name_basics_df, write_imdb_name_basics_df_to_csv
from name_df_cleaning_and_transformation import (convert_columns_to_snake_case,
                                                 convert_death_year_to_int,
                                                 expand_primary_profession,
                                                 calculate_age_at_death)

from name_df_dealing_with_nulls_and_dublicates import fill_missing_year_values, fill_missing_professions

if __name__ == '__main__':
    df = read_imdb_name_basics_df()
    #write_imdb_name_basics_df_to_csv(df, output_path="data/results/name.csv", num_rows=20, mode="overwrite")

    renamed_col_df = convert_columns_to_snake_case(df)
    transformed_dy_col_df = convert_death_year_to_int(renamed_col_df)
    fill_null_value_df = fill_missing_year_values(transformed_dy_col_df)
    expand_primary_profession_df = expand_primary_profession(fill_null_value_df)
    calculate_age_at_death_df = calculate_age_at_death(expand_primary_profession_df)
    fill_missing_professions_df = fill_missing_professions(calculate_age_at_death_df)

    fill_missing_professions_df.show(50, truncate=False)