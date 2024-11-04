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
from name_df_filtering import filter_actor_and_director, filter_casting_directors, filter_only_actor
from name_df_aggregation_and_grouping import (count_professions_by_group,
                                              analyze_titles_per_person,
                                              analyze_multi_profession_people)
from name_df_window_functions import analyze_career_versatility, analyze_profession_ranking

if __name__ == '__main__':
    df = read_imdb_name_basics_df()

    df = convert_columns_to_snake_case(df)
    df = convert_death_year_to_int(df)
    df = drop_years_columns(df)

    df = df.dropDuplicates()

    df_actors_and_directors = filter_actor_and_director(df)
    write_imdb_name_basics_df_to_csv(df_actors_and_directors, output_path="data/results/df_actors_and_directors.csv",
                                     mode="overwrite")

    df_casting_directors = filter_casting_directors(df)
    write_imdb_name_basics_df_to_csv(df_casting_directors, output_path="data/results/df_casting_directors.csv",
                                     mode="overwrite")

    df_only_actors = filter_only_actor(df)
    write_imdb_name_basics_df_to_csv(df_only_actors, output_path="data/results/df_only_actors.csv",
                                     mode="overwrite")

    count_professions_df = count_professions_by_group(df)
    write_imdb_name_basics_df_to_csv(count_professions_df, output_path="data/results/count_professions_df.csv",
                                     mode="overwrite")

    analyze_titles_per_person_df = analyze_titles_per_person(df)
    write_imdb_name_basics_df_to_csv(analyze_titles_per_person_df,
                                     output_path="data/results/analyze_titles_per_person_df.csv",
                                     mode="overwrite")

    analyze_multi_profession_people_df = analyze_multi_profession_people(df)
    write_imdb_name_basics_df_to_csv(analyze_multi_profession_people_df,
                                     output_path="data/results/analyze_multi_profession_people_df.csv",
                                     mode="overwrite")

    analyze_career_versatility_df = analyze_career_versatility(df)
    write_imdb_name_basics_df_to_csv(analyze_career_versatility_df,
                                     output_path="data/results/analyze_career_versatility_df.csv",
                                     mode="overwrite")

    analyze_profession_ranking_df = analyze_profession_ranking(df)
    write_imdb_name_basics_df_to_csv(analyze_profession_ranking_df,
                                     output_path="data/results/analyze_profession_ranking_df.csv",
                                     mode="overwrite")
