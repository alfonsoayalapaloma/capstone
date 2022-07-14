import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3



def run_moviereviews_etl():
    DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
    DATABASE_NAME="moviereviews.sqlite"
    movie_review_file="/data/var/incoming/capstone/movie_review.csv"
    USER_ID = ''
    TOKEN = ''

    # Convert time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000


    # Load
    df = pd.read_csv("/data/var/incoming/", sep=";") 
    #print(df.head())
    

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")






from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType





# Build a spark context
hc = (SparkSession.builder
                  .appName('Toxic Comment Classification')
                  .enableHiveSupport()
                  .config("spark.executor.memory", "4G")
                  .config("spark.driver.memory","18G")
                  .config("spark.executor.cores","7")
                  .config("spark.python.worker.memory","4G")
                  .config("spark.driver.maxResultSize","0")
                  .config("spark.sql.crossJoin.enabled", "true")
                  .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                  .config("spark.default.parallelism","2")
                  .getOrCreate())




def go_spark():
from pyspark.sql.types import StringType
from pyspark.sql.types import BooleanType
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import functions as F

tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
tokenized = tokenizer.transform(df)

remover = StopWordsRemover(inputCol="review_token", outputCol="text_filtered")
cleaned_document = remover.transform(tokenized)

isThereAGoodToken = udf(lambda words: "good" in words, BooleanType())

reviews =cleaned_document.withColumn("positive_review", isThereAGoodToken(col("text_filtered")))

df2=reviews.withColumn("positive_review",  F.when(
            F.col("positive_review") == True,
            1
        ).otherwise( 0 ) )

df3=df2.withColumn("insert_date", F.current_timestamp() )
df3.coalesce(1).select("cid",  "positive_review","id_review").write.csv('reviews2.csv')


def to_spark_df(fin):
    """
    Parse a filepath to a spark dataframe using the pandas api.

    Parameters
    ----------
    fin : str
        The path to the file on the local filesystem that contains the csv data.

    Returns
    -------
    df : pyspark.sql.dataframe.DataFrame
        A spark DataFrame containing the parsed csv data.
    """
    df = pd.read_csv(fin)
    df.fillna("", inplace=True)
    df = hc.createDataFrame(df)
    return(df)


df = to_spark_df("../input/moviereview/movie_review.csv")

