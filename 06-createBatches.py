import time, pickle, math
from sklearn.utils import shuffle
from pymongo import MongoClient

start = time.time()

connection = MongoClient('localhost', 27017, connect = False)
db = connection.pgc
pairs = db.pairs

batch_size = 1000
n_pairs = math.floor(pairs.count({'target': 1}) / batch_size) * batch_size
n_batches = int(n_pairs / batch_size)
last_id = ['', '']

print('batch_size:', batch_size, ', n_pairs:', n_pairs, ', n_batches:', n_batches)

for j in range(n_batches):
    start_batch = time.time()
    X = []
    y = []
    for i in range(2):
        cursor = pairs.find({ 'target': i, '_id': { '$gt': last_id[i] } })
        cursor.limit(batch_size)
        cursor.sort('_id', 1)
        for document in cursor:
            X = X + [document['features']]
            X = X + [document['features_inv']]
            y = y + [i] * 2
            last_id[i] = document['_id']
    X, y = shuffle(X, y)
    pickle.dump(X, open('./Batches/X.' + str(j), 'wb'))
    pickle.dump(y, open('./Batches/y.' + str(j), 'wb'))
    print('[', 100*(j+1)/n_batches, '% -', round(time.time() - start), '] - Batch', j+1, 'terminada em', round(time.time() - start_batch), 'segundos -', last_id)

print('Todas as batches terminadas em ', round(time.time() - start), 'segundos')