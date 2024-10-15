import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import data_path

def read_imbd_df(file_path=data_path):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Read IMDB DataFrame")
                     .config(conf=SparkConf())
                     .getOrCreate())

    imbd_schema = t.StructType([
        t.StructField("nconst", t.StringType(), True),
        t.StructField("primaryName", t.StringType(), True),
        t.StructField("birthYear", t.IntegerType(), True),
        t.StructField("deathYear", t.StringType(), True),
        t.StructField("primaryProfession", t.ArrayType(t.StringType()), True),
        t.StructField("knownForTitles", t.ArrayType(t.StringType()), True)
    ])

    df = spark_session.read.csv(file_path, header=True, nullValue='null', dateFormat='MM/dd/yyyy', schema=imbd_schema)
    return df
