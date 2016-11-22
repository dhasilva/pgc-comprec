import gzip
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
    file = gzip.open(path, 'r')
    for entry in file:
        item = eval(entry)
        item["_id"] = item.pop("asin")
        yield item


def populate_db(input_path):
    insert_counter = InsertCounter()
    connection = MongoClient('localhost', 27017, connect=False)
    db = connection.pgc
    items = db.itemsFull
    for item in parse(input_path):
        items.insert_one(item)
        insert_counter.increment()
    insert_counter.print_counter()


populate_db("meta_Clothing_Shoes_and_Jewelry.json.gz")
