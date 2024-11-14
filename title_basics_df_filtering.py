import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkConf

def filter_different_titles(df):
    """
    What records have different primary title and original title?
    """
    df = df.filter(col("primary_title") != col("original_title"))
    return df
