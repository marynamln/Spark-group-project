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


def filter_drama_movies(df):
    """
    What movies are in the drama genre?
    """
    df = df.withColumn("genres", f.split(col("genres"), ","))

    df = df.filter(
        (col("title_type") == "movie") &
        f.array_contains(col("genres"), "Drama")
    )

    df = df.withColumn("genres", f.concat_ws(",", col("genres")))
    return df

