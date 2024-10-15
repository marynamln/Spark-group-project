import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import data_path, path_to_save

def read_imdb_name_basics_df(file_path=data_path):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Read IMDB DataFrame")
                     .config(conf=SparkConf())
                     .getOrCreate())

    imbd_name_basics_schema = t.StructType([
        t.StructField("Nconst", t.StringType(), True),
        t.StructField("Primary Name", t.StringType(), True),
        t.StructField("Birth Year", t.IntegerType(), True),
        t.StructField("Death Year", t.StringType(), True),
        t.StructField("Primary Profession", t.StringType(), True),
        t.StructField("Known For Titles", t.StringType(), True)
    ])

    df = spark_session.read.csv(file_path, 
                                sep='\t',
                                header=True,
                                nullValue='null', 
                                dateFormat='MM/dd/yyyy', 
                                schema=imbd_name_basics_schema)
    return df

def write_imdb_name_basics_df_to_csv(df, output_path=path_to_save):
    df.write.csv(output_path, header=True, mode="overwrite")    