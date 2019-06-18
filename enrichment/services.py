from dataclasses import dataclass

from faker import Faker
import random

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

beers={'tuborg','goldstar','leff','corona','malt','kriek','blanche','888','camel','paulaner'}


def initProducts():
    faker = Faker()

    dict = {}
    i=1
    for beerName in beers:
        dict[i]=(Beer(name=beerName,price=random.randint(10,30)))
        i+=1
    return dict

@dataclass
class Beer:
    name:str
    price:int

class ProductService:
    dict=initProducts()



    def getBeerName(self,id):
        return self.dict[1]

    def beerNameById(id):udf(lambda id:ProductService.dict[id],StringType())


service = ProductService.dict[1]
name = ProductService.getBeerName(id=1)
print(name)

