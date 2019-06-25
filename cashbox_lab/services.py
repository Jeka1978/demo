import random

from cashbox_lab.model import Beer

beers = {'tuborg', 'goldstar', 'leff', 'corona', 'malt', 'kriek', 'blanche', '888', 'camel', 'paulaner'}


def initProducts():
    dict = {}
    i = 1
    for beerName in beers:
        dict[i] = (Beer(name=beerName, price=random.randint(10, 30)))
        i += 1
    return dict


class ProductService:
    dict = initProducts()

    def getBeerName(self, id):
        return self.dict[1]
