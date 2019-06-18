from pyspark.sql.session import SparkSession



class SparkHolder:
    spark = SparkSession.builder.master("local[*]").appName("linked in").getOrCreate()
