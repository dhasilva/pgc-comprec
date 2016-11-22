import struct
import time
from pymongo import MongoClient


class InsertCounter:
    def __init__(self, value=0):
        self.value = value

    def print_counter(self):
        print("%d items inseridos." % self.value)

    def increment(self):
        self.value += 1
        if self.value % 1000 == 0:
            self.print_counter()


def parse(path):
    file = open(path, 'rb')
    while True:
        _id = file.read(10).decode('utf-8')
        if _id == '':
            break
        features = []
        for i in range(4096):
            unpacked = struct.unpack('f', file.read(4))[0]
            features.append(unpacked)
        data_out = {"_id": _id, "features": features}
        yield data_out


def populate_db(input_path):
    insert_counter = InsertCounter()
    connection = MongoClient('localhost', 27017, connect=False)
    db = connection.pgc
    features = db.features
    for item in parse(input_path):
        features.insert_one(item)
        insert_counter.increment()
    insert_counter.print_counter()


start_time = time.time()
populate_db("image_features_Clothing_Shoes_and_Jewelry.b")
print("--- %s seconds ---" % round(time.time() - start_time, 2))
