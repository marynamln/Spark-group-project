import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def fill_missing_year_values(df):
    df = df.withColumn("death_year", f.when(df["death_year"].isNull(), 0).otherwise(df["death_year"]))
    df = df.withColumn("birth_year", f.when(df["birth_year"].isNull(), 0).otherwise(df["birth_year"]))

    return df

def fill_missing_professions(df):
    df = df.withColumn("profession_1", f.when(df["profession_1"].isNull(), None).otherwise(df["profession_1"]))
    df = df.withColumn("profession_2", f.when(df["profession_2"].isNull(), None).otherwise(df["profession_2"]))
    df = df.withColumn("profession_3", f.when(df["profession_3"].isNull(), None).otherwise(df["profession_3"]))

    return df