import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def filter_actor_and_director(df):
    """
    Who is an actor and a director at the same time?
    """
    df = df.filter((df["primary_profession"].contains("actor") |
                    df["primary_profession"].contains("actress")) &
                    df["primary_profession"].contains("director"))
    return df