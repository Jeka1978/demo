from os import getcwd

from pyspark import SparkContext
import re

from pyspark.sql import SparkSession

sc = SparkContext("local", "taxi")



def topX(path: str, x: int):
    with open(getcwd() + '\\garbage.txt') as f:
        stop_words = f.read().split(",")
    broadcast = sc.broadcast(stop_words)

    wordsRdd = sc.textFile(path).map(lambda line: line.lower()) \
        .map(lambda line: re.sub(r"[^a-zA-Z0-9]+", ' ', line)) \
        .flatMap(lambda line: line.split(' ')) \
        .filter(lambda word: len(word) > 1) \
        .filter(lambda word:word not in broadcast.value)\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda tuple: (tuple[1], tuple[0])) \
        .sortByKey(ascending=False)\


    for w in wordsRdd.collect():
        print(w)


topX("..\\songs\\pink floyd\\*", 3)
