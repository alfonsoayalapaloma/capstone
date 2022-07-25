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
GS_INPUT_FILE="gs://bucket-356805/log_reviews.csv"
GS_OUTPUT_FILE="gs://bucket-stage-356805/logreview"



def go_spark(hc):
    df = spark.read.format("csv") \
       .option("header","true") \
       .options(rowTag="post").load(GS_INPUT_FILE)
    df2 = df.selectExpr('id_review','log', "explode(xpath(log, 'reviewlog/log/logDate/text()')) as log_date")
    df3= df2.selectExpr('id_review','log','log_date'
                   ,"explode(xpath(log, 'reviewlog/log/device/text()')) as device"
                    )
    df4= df3.selectExpr('id_review','log','log_date', 'device'
                   ,"explode(xpath(log, 'reviewlog/log/location/text()')) as location"
                     )

    df5=df4.selectExpr( 'id_review','log','log_date', 'device', 'location'
                   ,"explode(xpath(log, 'reviewlog/log/ipAddress/text()')) as ip"
                  )
    df6=df5.selectExpr( 'id_review','log','log_date', 'device', 'location' ,'ip'
                   ,"explode(xpath(log, 'reviewlog/log/phoneNumber/text()')) as phone_number"
                   )
    df7=df6.selectExpr( 'id_review','log','log_date', 'device', 'location' ,'ip','phone_number'
                   ,"explode(xpath(log, 'reviewlog/log/os/text()')) as os"
                   )
    from pyspark.sql.functions import lit
    df8=df7.withColumn("browser",lit(""))
    df9=df8.selectExpr('id_review','log_date','device','os','location','browser','ip','phone_number')
    df9.coalesce(1).select('id_review','log_date','device','os','location','browser','ip','phone_number').write.csv( GS_OUTPUT_FILE )

if __name__ == "__main__":
    #if len(sys.argv) != 2:
    #    print("Usage: wordcount <file>", file=sys.stderr)
    #    sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("LogReview")\
        .getOrCreate()
    go_spark(spark)
    spark.stop()
