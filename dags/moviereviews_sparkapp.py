import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

import pyspark.sql.types as T
from pyspark.sql.types import StringType
from pyspark.sql.types import BooleanType
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import functions as F

from pyspark.sql import SparkSession

from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType


# local constants
INPUT_FILE_NAME = "/data/var/incoming/capstone/movie_review.csv"


def go_spark():
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

    df=to_spark_df(hc, INPUT_FILE_NAME)
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
    df3.coalesce(1).select("cid",  "positive_review","id_review").write.csv('/tmp/reviews2.csv')


def to_spark_df(hc, fin):
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



go_spark()
