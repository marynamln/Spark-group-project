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