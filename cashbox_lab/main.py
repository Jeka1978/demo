from pyspark.sql.types import StringType, IntegerType

from cashbox_lab.services import ProductService
from linkedIn.SparkUtils import SparkHolder
from pyspark.sql import functions as f

PRODUCT_ID = 'product_id'

CLIENT_ID = 'client_id'


@f.udf(returnType=StringType())
def getBeerName(id):
    return ProductService.dict[id].name




purchasesDf = SparkHolder.spark.read.json("input").alias('pp')
clients = SparkHolder.spark.read.json("clients.json")
join = purchasesDf.join(clients, purchasesDf[CLIENT_ID] == clients['id'])
join = join.withColumn('beer name', getBeerName(PRODUCT_ID))\
    .withColumn("beer price", ProductService.getBeerPrice(PRODUCT_ID))\
    .show()

#1. join dataframe from input file with cliens.json dataframe

