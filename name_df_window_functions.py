import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql.window import Window

def analyze_career_versatility(df):
    """
    How versatile are people in their professions compared to others with similar number of titles?
    """
    window_spec = Window.orderBy(f.size(f.split("known_for_titles", ",")))

    return (df.withColumn("title_count", f.size(f.split("known_for_titles", ",")))
              .withColumn("profession_count", f.size(f.split("primary_profession", ",")))
              .withColumn("versatility_rank", f.dense_rank().over(window_spec))
              .select("primary_name", "profession_count", "title_count", "versatility_rank")
              .orderBy(f.desc("profession_count")))


def analyze_profession_ranking(df):
    """
    What is each person's ranking within their primary profession based on number of titles?
    """
    window_spec = (Window.partitionBy(f.split("primary_profession", ",")[0])
                         .orderBy(f.size(f.split("known_for_titles", ",")).desc()))

    return (df.withColumn("title_count", f.size(f.split("known_for_titles", ",")))
              .withColumn("profession_rank", f.row_number().over(window_spec))
              .select("primary_name", "primary_profession", "title_count", "profession_rank")
              .orderBy("profession_rank"))