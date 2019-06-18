from pyspark.sql import functions as f

from enrichment.services import ProductService
from linkedIn.SparkUtils import SparkHolder

from pyspark.sql.functions import udf
@udf("long")
def squared_udf(s):
  return ProductService.dict[s].name

purchasesDf = SparkHolder.spark.read.json("input").alias('pp')
clients = SparkHolder.spark.read.json("clients.json")
join = purchasesDf.join(clients, f.col('client_id') == f.col('id'))
join.withColumn('product_name',squared_udf('product_id')).show()