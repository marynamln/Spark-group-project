import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf

def count_professions_by_group(df):
    """
    How many people have each combination of professions?
    """
    return (df.groupBy("primary_profession")
              .agg(f.count("*").alias("people_count"))
              .orderBy(f.col("people_count").desc()))

def analyze_titles_per_person(df):
    """
    How many titles is each person known for?
    """
    return (df.withColumn("title_count", f.size(f.split("known_for_titles", ",")))
              .groupBy("primary_name")
              .agg(
                  f.first("title_count").alias("number_of_titles"),
                  f.first("primary_profession").alias("professions")
              )
    )

def analyze_multi_profession_people(df):
    """
    How many people have multiple professions?
    """
    return (df.withColumn("profession_count", f.size(f.split("primary_profession", ",")))
              .groupBy("profession_count")
              .agg(f.count("*").alias("people_count"))
              .orderBy("profession_count")
    )