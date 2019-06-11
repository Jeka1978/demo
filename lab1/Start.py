from pyspark import SparkContext

from hw.model import Person, Math
from lab1.model import TaxiTrip

sc = SparkContext("local", "taxi")
rdd = sc.textFile("..\\taxi_orders.txt")
tripRdd = rdd.map(TaxiTrip.fromLine)
tripRdd.persist()
bostonRdd = tripRdd \
    .filter(lambda trip: trip.city == 'boston')
bostonRdd.persist()
longTripsToBostonAmount = bostonRdd \
    .filter(lambda trip: trip.km >= 10).count()

print(longTripsToBostonAmount)
totalToBoston = bostonRdd.map(lambda trip: trip.km).sum()
print("totalToBoston: " + str(totalToBoston))

totalKmPerDriverRdd = tripRdd.map(lambda trip: (trip.id, trip.km)) \
    .reduceByKey(lambda a, b: a + b).sortBy(lambda tuple:tuple[1],False)

driversInfoRdd=sc.textFile("..\\drivers.txt")\
    .map(lambda line:line.split(', '))\
    .map(lambda arr:(arr[0],arr[1]))

joinedRdd = totalKmPerDriverRdd.join(driversInfoRdd)

for j in joinedRdd.collect():
    print(j)

tripRdd.foreach(lambda trip:incCounter(trip.km))



smallTripsAmount = 0
avgTripsAmount = 0
longTripsAmount = 0
def incCounter(km:int):
    if(km<=5):
        smallTripsAmount+=1


#
# for t in tuples:
#     print(t)

#
# line = "113 Dorn 69 21/05/19"
# trip = TaxiTrip.fromLine(line)
# print(trip)
