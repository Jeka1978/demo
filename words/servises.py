from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import Row




class TopWordService:
    def f(x):
        d = {}
        for i in range(len(x)):
            d[str(i)] = x[i]
        return d


    def topX(self,fileName:str,x:int,sc:SparkContext):
        df = sc.textFile(fileName).flatMap(lambda line: line.split("\\W+")).map(lambda x: Row(**self.f(x))).toDF()
        df.show()






service = TopWordService()
service.topX("..\\taxi_orders.txt",3,sc)