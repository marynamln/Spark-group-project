import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import data_path_title_crew, data_path_title_basics

def read_imdb_title_crew_df(file_path=data_path_title_crew):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Read IMDB DataFrame")
                     .config(conf=SparkConf())
                     .getOrCreate())

    imbd_title_crew_schema = t.StructType([
        t.StructField("tconst", t.StringType(), True),
        t.StructField("directors", t.StringType(), True),
        t.StructField("writers", t.StringType(), True)
    ])

    df = spark_session.read.csv(file_path,
                                sep='\t',
                                header=True,
                                nullValue='null',
                                schema=imbd_title_crew_schema)
    return df

def read_imdb_title_basics_df(file_path=data_path_title_basics):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Read IMDB DataFrame")
                     .config(conf=SparkConf())
                     .getOrCreate())

    imbd_title_basics_schema = t.StructType([
        t.StructField("tconst", t.StringType(), True),
        t.StructField("title_type", t.StringType(), True),
        t.StructField("primary_title", t.StringType(), True),
        t.StructField("original_title", t.StringType(), True),
        t.StructField("is_adult", t.StringType(), True),
        t.StructField("start_year", t.IntegerType(), True),
        t.StructField("end_year", t.IntegerType(), True),
        t.StructField("runtime_minutes", t.IntegerType(), True),
        t.StructField("genres", t.StringType(), True)
    ])

    df = spark_session.read.csv(file_path,
                                sep='\t',
                                header=True,
                                nullValue='null',
                                schema=imbd_title_basics_schema)
    return df