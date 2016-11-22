var ids = new Set();
var processedCount = 0;
var pairsCount = 0;
var batchSize = 1000;
var lastId = '';
var stop;
var start;
var results;
var running = true;
var boughtTogether;
var alsoViewed;
var id;
var featuresA;
var featuresB;
var pairId;
var pair;
var numberPairs;
var currentBatch = 0;
var currentExtraBatch = 0;
var pairIds;

function makeId(idA, idB) {
    if(idA < idB) {
        return idA.concat(idB);
    }
    else {
        return idB.concat(idA);
    }
}

function separateIds(id) {
    return [id.substring(0,10), id.substring(10,20)];
}

function getSetLength(set) {
    var length = 0;
    set.forEach(function(entry) {
        length += 1;
    });
    return length;
}

function makePair(id, featuresA, featuresB, target) {
    pair = {
        _id: id, // String de 20 posições contendo a _id dos dois itens em ordem
        features: featuresA.concat(featuresB),
        features_inv: featuresB.concat(featuresA),
        target: target
    };
    return pair;
}

function insertPair(itemPair) {
    try {
        db.pairs.insertOne(itemPair, { writeConcern: { w : "majority", j: 1 } });
        pairsCount += 1;
    }
    catch(error) {
        if (error.errmsg.substring(0, 6) !== "E11000") {
            print (error.errmsg);
        }
    }
}

function getTime() {
    stop = new Date().getTime();
    var totalMilliseconds = stop - start;
    var hours = Math.floor(((totalMilliseconds / 1000) / 60) / 60);
    var hoursMilliseconds = hours * 60 * 60 * 1000;
    var minutes = Math.floor(((totalMilliseconds - hoursMilliseconds) / 1000) / 60);
    var minutesMilliseconds = minutes * 60 * 1000;
    var seconds = Math.floor((totalMilliseconds - hoursMilliseconds - minutesMilliseconds) / 1000);
    var secondsMilliseconds = seconds * 1000;
    var milliseconds = totalMilliseconds - hoursMilliseconds - minutesMilliseconds - secondsMilliseconds;
    var message = ' #' + hours + ':' + minutes + ':' + seconds + '.' + milliseconds;
    totalMilliseconds = null;
    hours = null;
    hoursMilliseconds = null;
    minutes = null;
    minutesMilliseconds = null;
    seconds = null;
    secondsMilliseconds = null;
    milliseconds = null;
    return message;
}

function showProgress() {
    if(processedCount === 1 || processedCount % 100 === 0) {
        print(processedCount + ' itens processados e ' + pairsCount + ' pares inseridos' + getTime());
    }
}

function processItem(item, strict){
    id = item._id;
    item.related = item.related || {};
    boughtTogether = item.related.bought_together || [];
    alsoViewed = item.related.also_viewed || [];
    featuresA = null;
    
    // Classe 0 - Also Viewed
    alsoViewed.forEach(function(avid) {
        // Se está em boughtTogether, desconsiderar
        if(boughtTogether.indexOf(avid) !== -1) {
            return;
        }
        // Se não está em ids, desconsiderar caso seja possível
        if(strict && !ids.has(avid)) {
            return;
        }

        pairId = makeId(id, avid);
        featuresA = featuresA || db.features.findOne({_id: id}, { _id: 0, features: 1 }).features;
        featuresB = db.features.findOne({_id: avid}, { _id: 0, features: 1 }).features;
        pair = makePair(pairId, featuresA, featuresB, 0);
        return insertPair(pair);
    });
    
    processedCount += 1;
    return showProgress();
}

function begin() {
    try {
        start = new Date().getTime();
        numberPairs = Math.floor(db.pairs.count() / 1000) * 1000;
        for(currentBatch = 0; currentBatch < numberPairs / batchSize; currentBatch += 1) {
            db.pairs.find({ _id: { $gt: lastId } }, { _id: 1 }).limit(batchSize).forEach(function(item) {
                lastId = item._id;
                pairIds = separateIds(lastId);
                ids.add(pairIds[0]);
                ids.add(pairIds[1]);
            });
            print('Batch ' + currentBatch + ' adicionada ao Set (' + getSetLength(ids) + ')' + getTime());
        }
    }
    catch(error) {
        print('ERRO na batch ' + currentBatch + '.' + getTime());
        print (error);
    }
    try {
        currentBatch = 0;
        lastId = '';
        while(pairsCount < numberPairs && running) {
            results = db.items.find({ _id: { $gt: lastId } }).limit(batchSize).toArray();
            if(results.length === 0) {
                running = false;
                continue;
            }
            results.forEach(function(item) {
                lastId = item._id;
                if(ids.has(item._id)) {
                    processItem(item, true);
                }
            });
            print('Batch ' + currentBatch + ' executada. lastId = ' + lastId + getTime());
            currentBatch += 1;
        }
        // Caso não tenha sido possível usar pares que só envolvam ids com pares classe 1, usar pares que tenham ao menos um id de pares classe 1
        if(pairsCount < numberPairs) {
            print('Não há pares o suficiente com ambos os ids da classe 1. Adicionando pares com um id de par classe 1.');
            lastId = '';
            currentExtraBatch = 0;
            while(pairsCount < numberPairs) {
                db.items.find({ _id: { $gt: lastId } }).limit(batchSize).forEach(function(item) {
                    lastId = item._id;
                    if(ids.has(item._id)) {
                        processItem(item, false);
                    }
                });
                print('Batch ' + currentBatch + ' + ' + currentExtraBatch + ' executada. lastId = ' + lastId + getTime());
                currentExtraBatch += 1;
            }
        }

        print('Todos os pares inseridos com sucesso' + getTime());
        print('-----------------');
        print('Iniciando criação do índice { target: 1, _id: 1 }');
        db.pairs.createIndex({ target: 1, _id: 1});
        print('Índice criado com sucesso.');
    }
    catch(error) {
        print('ERRO com ' + processedCount + ' itens processados, ' + pairsCount + ' pares processados. Na batch ' + currentBatch + ' + ' + currentExtraBatch + '.' + getTime());
        print (error);
    }
}
