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
GS_INPUT_FILE="gs://bucket-356805/movie_review.csv"
GS_OUTPUT_FILE="gs://bucket-stage-356805/moviereview"



def go_spark(hc):
    df=hc.read.option("header",True).csv(GS_INPUT_FILE) 
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
    df3.coalesce(1).select("cid",  "positive_review","id_review").write.csv( GS_OUTPUT_FILE )

if __name__ == "__main__":
    #if len(sys.argv) != 2:
    #    print("Usage: wordcount <file>", file=sys.stderr)
    #    sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("MovieReview")\
        .getOrCreate()
    go_spark(spark)
    spark.stop()
