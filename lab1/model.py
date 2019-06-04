import dataclasses


@dataclasses.dataclass
class TaxiTrip:
    id: str
    city: str
    km: int

    @staticmethod
    def fromLine(line):
        list = line.split(" ")
        return TaxiTrip(list[0], list[1], int(list[2]))
