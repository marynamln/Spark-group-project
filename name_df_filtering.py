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

def filter_casting_directors(df):
    """
    Who has a casting director profession with three or more titles?
    """
    df = df.withColumn("primary_profession", f.split(df["primary_profession"], ","))
    df = df.withColumn("known_for_titles", f.split(df["known_for_titles"], ","))

    df = df.filter(f.array_contains(df.primary_profession, "casting_director") &
                  (f.size(df.known_for_titles) >= 3))

    df = df.withColumn("primary_profession", f.concat_ws(",", "primary_profession"))
    df = df.withColumn("known_for_titles", f.concat_ws(",", "known_for_titles"))

    return df

def filter_only_actor(df):
    """
    Which persons have only the profession of actor/actress?
    """
    df = df.withColumn("primary_profession", f.split(df["primary_profession"], ","))

    df = df.filter(
        (f.size("primary_profession") == 1) &
        (f.array_contains("primary_profession", "actor") |
         f.array_contains("primary_profession", "actress"))
    )

    df = df.withColumn("primary_profession", f.concat_ws(",", "primary_profession"))
    return df