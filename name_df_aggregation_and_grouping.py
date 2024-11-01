import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def count_professions_by_group(df):
    """
    How many people have each combination of professions?
    Uses aggregation and grouping, with sorting by count descending.
    """
    return (df.groupBy("primary_profession")
              .agg(f.count("*").alias("people_count"))
              .orderBy(f.col("people_count").desc()))