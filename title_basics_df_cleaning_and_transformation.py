import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def make_columns_snake_case(df):
    new_columns = [col.lower().replace(" ", "_") for col in df.columns]

    for idx, old_col in enumerate(df.columns):
        df = df.withColumnRenamed(old_col, new_columns[idx])

    return df