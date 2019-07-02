from os import getcwd
from pyspark.sql import functions as func
from pyspark import SparkContext
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField

sc = SparkContext("local", "taxi")
spark = SparkSession(sparkContext=sc)


def topX(path: str, x: int):
    schema = StructType(
        [StructField("word", StringType(), True)])

    with open(getcwd() + '\\garbage.txt') as f:
        stop_words = f.read().split(",")
    broadcast = sc.broadcast(stop_words)

    wordsRdd = sc.textFile(path).map(lambda line: line.lower()) \
        .map(lambda line: re.sub(r"[^a-zA-Z0-9]+", ' ', line)) \
        .flatMap(lambda line: line.split(' ')) \
        .filter(lambda word: len(word) > 1)\
        .map(lambda word:(word,1))
    spark.createDataFrame(wordsRdd, schema=['word','amount'])\
        .filter(~ func.col('word').isin(broadcast.value))\
        .groupBy('word')\
        .agg(func.sum(func.col('amount')).alias("total"))\
        .orderBy('total',ascending=False).select(['word','total']).show()





topX("..\\songs\\pink floyd\\*", 3)
