import pyspark.sql.types as t
import pyspark.sql.functions as f


def get_comedy_directors(name_df, title_crew_df, title_basics_df):
    """
    Which directors have made the most films in the comedy genre?
    """
    name_df = name_df.withColumn("primary_profession", f.split(f.col("primary_profession"), ","))

    directors_df = name_df.filter(f.array_contains(f.col("primary_profession"), "director"))

    exploded_crew_df = title_crew_df.withColumn("director_nconst", f.explode(f.split(f.col("directors"), ",")))

    directors_df = (exploded_crew_df
                    .join(directors_df, exploded_crew_df["director_nconst"] == directors_df["nconst"], how="inner")
                    .select("director_nconst", "primary_name", "tconst"))

    comedy_directors_df = (directors_df
                           .join(title_basics_df
                                 .withColumn("genres", f.split(f.col("genres"), ","))
                                 .filter(f.array_contains(f.col("genres"), "Comedy")), on="tconst", how="inner")
                           .groupBy("primary_name")
                           .agg(f.count("tconst").alias("comedy_count"))
                           .orderBy(f.col("comedy_count").desc())
                           )

    return comedy_directors_df