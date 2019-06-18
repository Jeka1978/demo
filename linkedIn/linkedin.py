# from pyspark.sql import SparkSession, functions as f
#
# from linkedIn.SparkUtils import SparkHolder
#
# SALARY = "salary"
#
#
# df = SparkHolder.spark.read.json("profiles.json")
# # df.registerTempTable('prof')
# # SparkHolder.spark.sql('select * from prof where prof.age>30').show()
# salaryDf = df.withColumn(SALARY, f.when(df.age > 30, df.age * 2).otherwise(df.age) * 10 * f.size('keywords'))
# salaryDf.show()
# mostPopular = salaryDf.withColumn("keyword", f.explode('keywords')) \
#     .select('keyword') \
#     .groupBy('keyword').agg(f.count('keyword').alias('amount')) \
#     .orderBy(f.col('amount').desc()).head().keyword
#
# salaryDf.filter(f.array_contains(salaryDf.keywords, mostPopular)).filter(salaryDf.salary < 1200).show()
