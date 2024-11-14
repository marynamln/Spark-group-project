import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def calculate_missing_values_percentage(df):
    total_rows = df.count()
    missing_values = df.select([(f.count(f.when(f.col(c).isNull(), c)) / total_rows * 100).alias(c) for c in df.columns])
    return missing_values

def drop_end_year_column(df):
    df = df.drop("end_year")
    return df