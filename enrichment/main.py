from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from enrichment.services import ProductService
from linkedIn.SparkUtils import SparkHolder

from pyspark.sql.functions import udf
@udf()
def squared_udf(s):
  return ProductService.dict[s].name

@udf(returnType=StringType())
def mix_udf(s,w):
  return s+w




purchasesDf = SparkHolder.spark.read.json("input").alias('pp')
clients = SparkHolder.spark.read.json("clients.json")
join = purchasesDf.join(clients, f.col('client_id') == f.col('id'))
join.withColumn('product_name',squared_udf('product_id'))\
  .groupBy(f.window("timestamp", "1 seconds")).agg(sum("client_id").alias("sum")).show()