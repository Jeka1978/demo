from linkedIn.SparkUtils import SparkHolder
from pyspark.sql import functions as f

CLIENT_ID = 'client_id'

purchasesDf = SparkHolder.spark.read.json("input").alias('pp')
clients = SparkHolder.spark.read.json("clients.json")
join = purchasesDf.join(clients, purchasesDf[CLIENT_ID] == clients['id'])
join = join.withColumn('abc',f.ud(1))
join.show()

#1. join dataframe from input file with cliens.json dataframe

