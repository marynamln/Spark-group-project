import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def convert_columns_to_snake_case(df):
    new_columns = [col.lower().replace(" ", "_") for col in df.columns]

    for idx, old_col in enumerate(df.columns):
        df = df.withColumnRenamed(old_col, new_columns[idx])

    return df

def convert_death_year_to_int(df):
    return df.withColumn("death_year", df["death_year"].cast("int"))

def fill_missing_values(df):
    df = df.withColumn("death_year", f.when(df["death_year"].isNull(), 0).otherwise(df["death_year"]))
    df = df.withColumn("birth_year", f.when(df["birth_year"].isNull(), 0).otherwise(df["birth_year"]))

    return df

def expand_primary_profession(df):
    professions = f.split(df["primary_profession"], ",")
    return df.withColumn("profession_1", professions.getItem(0)) \
             .withColumn("profession_2", professions.getItem(1)) \
             .withColumn("profession_3", professions.getItem(2))
