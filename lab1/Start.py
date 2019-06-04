from pyspark import SparkContext

from hw.model import Person, Math
from lab1.model import TaxiTrip

sc = SparkContext("local", "taxi")
rdd = sc.textFile("..\\taxi_orders.txt")
longTripsToBostonAmount = rdd \
    .map(lambda line: line.lower()) \
    .map(TaxiTrip.fromLine) \
    .filter(lambda trip: trip.city == 'boston') \
    .filter(lambda trip: trip.km > 10).count()

print(longTripsToBostonAmount)

#
# line = "113 Dorn 69 21/05/19"
# trip = TaxiTrip.fromLine(line)
# print(trip)


