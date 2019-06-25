from pyspark.sql import functions as ff

from linkedIn.SparkUtils import SparkHolder

AGE = 'age'
AGE_COL = ff.col(AGE)

JAVA = 'java'

AMOUNT = "amount"

KEYWORD = 'keyword'

KEYWORDS = 'keywords'

SALARY = "salary"

df = SparkHolder.spark.read.json("profiles.json")
df.show()
df.toJSON().saveAsTextFile("c:\\tmp\\abc.json")

df.printSchema()
fields = df.schema.fields
for f in fields:
    print(f.name)


df.registerTempTable("rowDf")
SparkHolder.spark.sql("select * from rowDf where age>30").show()

salaryDf = df.withColumn(SALARY, ff.when(AGE_COL < 30, AGE_COL).otherwise(AGE_COL * 2) * ff.size(KEYWORDS) * 10)

salaryDf.show()

# df.withColumn(KEYWORD, ff.explode(KEYWORDS)).select(KEYWORD).show()
keywordDf = df.select(ff.explode(KEYWORDS).alias(KEYWORD)).groupBy(KEYWORD).agg(ff.count(KEYWORD).alias(AMOUNT))
keywordDf.show()
# keywordDf.orderBy(AMOUNT).show()
keywordSortedDf = keywordDf.orderBy(ff.col(AMOUNT).desc())
# keywordDf.orderBy(keywordDf.amount).show()
# keywordDf.orderBy(keywordDf[AMOUNT].desc()).show()
# df.select(ff.explode(KEYWORDS).alias(KEYWORD)).groupBy(KEYWORD).count().show()
keywordSortedDf.show()
row = keywordSortedDf.head()
# mostPopuplar = row[0]
# mostPopuplar = row.keyword
# mostPopuplar = row[KEYWORD]
mostPopuplar = row[KEYWORD]



print(mostPopuplar)

salaryDf.where(ff.col(SALARY) < 1200).where(ff.array_contains(KEYWORDS,mostPopuplar)).show()




