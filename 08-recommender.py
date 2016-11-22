import time
from pymongo import MongoClient, ReturnDocument
from sklearn.externals import joblib
from math import ceil
from bisect import bisect
from sklearn.metrics.pairwise import cosine_similarity

connection = MongoClient('localhost', 27017, connect = False)
db = connection.pgc
n_items = db.items.count()

path = '.\Classificadores\\'

classifiers = [
    ('clfP', joblib.load(path + 'clfP\\' + '509.clf')),
    ('clfPL1', joblib.load(path + 'clfPL1\\' + '509.clf')),
    ('clfSGDCL1', joblib.load(path + 'clfSGDCL1\\' + '509.clf')),
    ('clfSGDO', joblib.load(path + 'clfSGDO\\' + '509.clf')),
    ('clfSGDOL1', joblib.load(path + 'clfSGDOL1\\' + '509.clf')),
    ('clfSGDOLOG', joblib.load(path + 'clfSGDOLOG\\' + '509.clf')),
    ('clfPAH', joblib.load(path + 'clfPAH\\' + '509.clf')),
    ('clfPASH', joblib.load(path + 'clfPASH\\' + '509.clf'))
]

def get_seconds(start):
    end = time.time()
    elapsed = end - start
    return elapsed

def log(line):
    f = open('./recommender.log', 'a')
    f.write(line + '\n')
    f.close()
    print(line)

def get_items(last_id=''):
    batch_size = 1000
    n_batches = ceil(n_items / batch_size)
    for i in range(n_batches):
        cursor = db.items.find({ '_id': { '$gt': last_id } }, { '_id': 1, 'recommended': 1, 'is_processed': 1 })
        cursor.limit(batch_size)
        cursor.sort('_id', 1)
        
        cursor_f = db.features.find({ '_id': { '$gt': last_id } })
        cursor_f.limit(batch_size)
        cursor_f.sort('_id', 1)
        
        for document, features in zip(cursor, cursor_f):
            if(document['_id'] != features['_id']):
                message = 'Ids ' + str(document['_id']) + ' e ' + str(features['_id']) + ' não batem!'
                log(message)
                raise Exception(message)
            last_id = document['_id']
            document['features'] = features['features']
            yield document
            
def get_items_from_list(ids):
    cursor = db.items.find({ '_id': { '$in': ids } }, { '_id': 1, 'recommended': 1, 'is_processed': 1 })
    cursor.sort('_id', 1)
    retrieved_items = list(cursor)
    cursor_f = db.features.find({ '_id': { '$in': ids } })
    cursor_f.sort('_id', 1)
    retrieved_features = list(cursor_f)
    for document, features in zip(retrieved_items, retrieved_features):
        if(document['_id'] != features['_id']):
            message = 'Ids ' + str(document['_id']) + ' e ' + str(features['_id']) + ' não batem!'
            log(message)
            raise Exception(message)
        document['features'] = features['features']
        yield document

def insert_score_ordered(ordered_recommended, element, first):
    new_list = list(ordered_recommended)
    recommended_keys = [k['score'] for k in new_list]
    index = bisect(recommended_keys, element['score'])
    if(len(recommended_keys) == 0 or recommended_keys[index - 1] != element['score']):
        new_list.insert(index, element)
    if(first == True):
        return new_list[:20]
    else:
        return new_list[-20:]
        
def insert_score_ordered_N(ordered_recommended, element, first):
    new_list = list(ordered_recommended)
    recommended_keys = [k['score'] for k in new_list]
    index = bisect(recommended_keys, element['score'])
    if(len(recommended_keys) == 0 or recommended_keys[index - 1] != element['score']):
        new_list.insert(index, element)
    if(first == True):
        return new_list[:20000]
    else:
        return new_list[-20000:]

def calculate_ETA(current, time):
    return str((n_items * time / current) - time)

def calculate_recommended_cos(item):
    idA = item['_id']
    featuresA_doc = db.features.find_one({'_id': idA})
    if(featuresA_doc == None):
        message = 'Item ' + str(idA) + ' não existe!'
        log(message)
        raise Exception(message)
    featuresA = featuresA_doc['features']
    if('recommended') not in item:
        item['recommended'] = {}
        item['recommended']['class0'] = {}
        item['recommended']['class1'] = {}
        for name, clf in classifiers:
            item['recommended']['class0'][name] = []
            item['recommended']['class1'][name] = []
        item['recommended']['cos_similar'] = []
        item['recommended']['cos_dissimilar'] = []
        
    log('-- Calculando itens similares por cosseno para o item ' + str(idA) + ' --')
    for i, itemB in enumerate(get_items()):
        idB = itemB['_id']
        if(itemB['is_processed'] == 0 and idB != idA):
            featuresB = itemB['features']
            score_cos = cosine_similarity([featuresA], [featuresB])[0][0]
            element_cos = {'_id': idB, 'score': score_cos}
            # Adicionando semelhança de cossenos ao item A
            item['recommended']['cos_similar'] = insert_score_ordered_N(item['recommended']['cos_similar'], element_cos, first = False)
            item['recommended']['cos_dissimilar'] = insert_score_ordered_N(item['recommended']['cos_dissimilar'], element_cos, first = True)
        if(i == 0 or (i + 1) % 1000 == 0):
            partial_time = get_seconds(start_all)
            log(str(partial_time) + ' | ' + str(i + 1) + ' itens calculados de ' + str(n_items) + '. ETA = ' + calculate_ETA(i + 1, partial_time))
    # Salva no banco
    log(str(get_seconds(start_all)) + ' | Todos os itens calculados!')
    itemDone = db.items.find_one_and_update({ '_id': idA }, { '$set': { 'recommended': item['recommended'], 'is_processed': 1 } }, upsert = False, return_document = ReturnDocument.AFTER)
    log(str(get_seconds(start_all)) + ' | Salvo no banco de dados')
    return itemDone
            
def calculate_recommended(item):
    item = calculate_recommended_cos(item)
    idA = item['_id']
    featuresA_doc = db.features.find_one({'_id': idA})
    if(featuresA_doc == None):
        message = 'Item ' + str(idA) + ' não existe!'
        log(message)
        raise Exception(message)
    featuresA = featuresA_doc['features']
    log('-- Calculando itens recomendados para o item ' + str(idA) + ' --')
    id_list = [x['_id'] for x in item['recommended']['cos_similar']]
    for i, itemB in enumerate(get_items_from_list(id_list)):
        idB = itemB['_id']
        featuresB = itemB['features']
        if(idB != idA):
            for name, clf in classifiers:
                score_clf = clf.decision_function([featuresA + featuresB])[0]
                element_clf = {'_id': idB, 'score': score_clf}
                if(clf.predict([featuresA + featuresB])[0] == 1):
                    item['recommended']['class1'][name] = insert_score_ordered(item['recommended']['class1'][name], element_clf, first = False)
        if(i == 0 or (i + 1) % 1000 == 0):
            partial_time = get_seconds(start_all)
            log(str(partial_time) + ' | ' + str(i + 1) + ' itens calculados de ' + str(n_items) + '. ETA = ' + calculate_ETA(i + 1, partial_time))
    
    for i, itemB in enumerate(get_items_from_list(item['related']['also_viewed'])):
        idB = itemB['_id']
        featuresB = itemB['features']
        if(idB != idA):
            for name, clf in classifiers:
                score_clf = clf.decision_function([featuresA + featuresB])[0]
                element_clf = {'_id': idB, 'score': score_clf}
                if(clf.predict([featuresA + featuresB])[0] == 0):
                    item['recommended']['class0'][name] = insert_score_ordered(item['recommended']['class0'][name], element_clf, first = True)
        if(i == 0 or (i + 1) % 1000 == 0):
            partial_time = get_seconds(start_all)
            log(str(partial_time) + ' | ' + str(i + 1) + ' itens calculados de ' + str(n_items) + '. ETA = ' + calculate_ETA(i + 1, partial_time))
    
    # Salva no banco
    log(str(get_seconds(start_all)) + ' | Todos os itens calculados!')
    itemDone = db.items.find_one_and_update({ '_id': idA }, { '$set': { 'recommended': item['recommended'], 'is_processed': 2 } }, upsert = False, return_document = ReturnDocument.AFTER)
    log(str(get_seconds(start_all)) + ' | Salvo no banco de dados')
    return itemDone
            
def recommend(item):
    if(item['is_processed'] != 2):
        item = calculate_recommended(item)
    return sorted(item['recommended']['class1']['clfSGDOL1'], reverse = True, key = lambda k: k['score'])

item = db.items.find_one({'is_processed': 0})
start_all = time.time()
recommend(item)
