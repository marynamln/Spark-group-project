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

from io_help_functions import read_imdb_title_crew_df, read_imdb_title_basics_df, read_imdb_title_principals_df

from name_df_joins import get_comedy_directors, get_actor_roles_count

if __name__ == '__main__':
    df_name_basics = read_imdb_name_basics_df()

    df_name_basics = convert_columns_to_snake_case(df_name_basics)
    df_name_basics = convert_death_year_to_int(df_name_basics)
    df_name_basics = drop_years_columns(df_name_basics)

    df_name_basics = df_name_basics.dropDuplicates()

    df_title_crew = read_imdb_title_crew_df()
    df_title_basics = read_imdb_title_basics_df()
    df_title_principals = read_imdb_title_principals_df()

    df_actors_and_directors = filter_actor_and_director(df_name_basics)
    write_imdb_name_basics_df_to_csv(df_actors_and_directors, output_path="data/results/df_actors_and_directors.csv",
                                     mode="overwrite")

    df_casting_directors = filter_casting_directors(df_name_basics)
    write_imdb_name_basics_df_to_csv(df_casting_directors, output_path="data/results/df_casting_directors.csv",
                                     mode="overwrite")

    df_only_actors = filter_only_actor(df_name_basics)
    write_imdb_name_basics_df_to_csv(df_only_actors, output_path="data/results/df_only_actors.csv",
                                     mode="overwrite")

    count_professions_df = count_professions_by_group(df_name_basics)
    write_imdb_name_basics_df_to_csv(count_professions_df, output_path="data/results/count_professions_df.csv",
                                     mode="overwrite")

    analyze_titles_per_person_df = analyze_titles_per_person(df_name_basics)
    write_imdb_name_basics_df_to_csv(analyze_titles_per_person_df,
                                     output_path="data/results/analyze_titles_per_person_df.csv",
                                     mode="overwrite")

    analyze_multi_profession_people_df = analyze_multi_profession_people(df_name_basics)
    write_imdb_name_basics_df_to_csv(analyze_multi_profession_people_df,
                                     output_path="data/results/analyze_multi_profession_people_df.csv",
                                     mode="overwrite")

    analyze_career_versatility_df = analyze_career_versatility(df_name_basics)
    write_imdb_name_basics_df_to_csv(analyze_career_versatility_df,
                                     output_path="data/results/analyze_career_versatility_df.csv",
                                     mode="overwrite")

    analyze_profession_ranking_df = analyze_profession_ranking(df_name_basics)
    write_imdb_name_basics_df_to_csv(analyze_profession_ranking_df,
                                     output_path="data/results/analyze_profession_ranking_df.csv",
                                     mode="overwrite")

    df_comedy_directors = get_comedy_directors(df_name_basics, df_title_crew, df_title_basics)
    write_imdb_name_basics_df_to_csv(df_comedy_directors,
                                     output_path="data/results/df_comedy_directors.csv",
                                     mode="overwrite")

    df_actor_roles_count = get_actor_roles_count(df_name_basics, df_title_principals)
    write_imdb_name_basics_df_to_csv(df_actor_roles_count,
                                     output_path="data/results/df_actor_roles_count.csv",
                                     mode="overwrite")