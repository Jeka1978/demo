import random

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from cashbox_lab.model import Beer

beers = {'tuborg', 'goldstar', 'leff', 'corona', 'malt', 'kriek', 'blanche', '888', 'camel', 'paulaner'}


def initProducts():
    products = {}
    i = 1
    for beerName in beers:
        products[i] = (Beer(name=beerName, price=random.randint(10, 30)))
        i += 1
    return products


class ProductService:
    dict = initProducts()


    @udf(returnType=IntegerType())
    def getBeerPrice(id):
        return ProductService.dict[id].price

#
# service = ProductService()
# beer = service.getBeerName(2)
# print(beer.name)
# print(beer.price)
