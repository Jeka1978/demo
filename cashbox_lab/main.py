from pyspark.sql.types import StringType, IntegerType

from cashbox_lab.services import ProductService
from linkedIn.SparkUtils import SparkHolder
from pyspark.sql import functions as f

BEER_NAME = 'beer name'

CORRELATION = "correlation"

PRODUCT_ID = 'product_id'

CLIENT_ID = 'client_id'


@f.udf(returnType=StringType())
def getBeerName(id):
    return ProductService.dict[id].name

@f.udf(returnType=IntegerType())
def getBeerToNameCorrelation(beer_name,client_name):
    return len(beer_name)+len(client_name)


purchasesDf = SparkHolder.spark.read.json("input").alias('pp')
clients = SparkHolder.spark.read.json("clients.json")
join = purchasesDf.join(clients, purchasesDf[CLIENT_ID] == clients['id'])
join = join.withColumn(BEER_NAME, getBeerName(PRODUCT_ID))\
    .withColumn("beer price", ProductService.getBeerPrice(PRODUCT_ID))\
    .withColumn(CORRELATION,getBeerToNameCorrelation(BEER_NAME,'name')).show()

#1. join dataframe from input file with cliens.json dataframe

