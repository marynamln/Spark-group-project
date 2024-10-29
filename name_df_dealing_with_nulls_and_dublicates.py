import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def drop_years_columns(df):
    df = df.drop("birth_year", "death_year")
    return df

def fill_missing_professions(df):
    df = df.withColumn("profession_1", f.when(df["profession_1"].isNull(), None).otherwise(df["profession_1"]))
    df = df.withColumn("profession_2", f.when(df["profession_2"].isNull(), None).otherwise(df["profession_2"]))
    df = df.withColumn("profession_3", f.when(df["profession_3"].isNull(), None).otherwise(df["profession_3"]))

    return df