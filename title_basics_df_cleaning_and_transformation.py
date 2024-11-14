import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def make_columns_snake_case(df):
    new_columns = [col.lower().replace(" ", "_") for col in df.columns]

    for idx, old_col in enumerate(df.columns):
        df = df.withColumnRenamed(old_col, new_columns[idx])

    return df


def convert_data_types(df):
    df = df.withColumn("start_year", df["start_year"].cast("int"))
    df = df.withColumn("end_year", df["end_year"].cast("int"))
    df = df.withColumn("runtime_minutes", df["runtime_minutes"].cast("int"))
    df = df.withColumn("is_adult", f.when(f.col("is_adult") == "1", True).otherwise(False))
    return df

def fill_in_missing_values(df):
    df = df.withColumn("start_year", f.when(df["start_year"].isNull(), 0).otherwise(df["start_year"]))
    df = df.withColumn("runtime_minutes", f.when(df["runtime_minutes"].isNull(), 0).otherwise(df["runtime_minutes"]))
    return df