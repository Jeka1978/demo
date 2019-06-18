from pyspark import SparkContext, SparkConf
from pyspark.sql.types import Row


conf = SparkConf().setMaster("local[*]").setAppName("taxi")
sc = SparkContext(conf=conf)

#here you are going to create a function
def f(x):
    d = {}
    for i in range(len(x)):
        d[str(i)] = x[i]
    return d

#Now populate that
rdd = sc.parallelize(["aaa","aff","q"])
df = rdd.map(lambda x: Row(**f(x))).toDF()