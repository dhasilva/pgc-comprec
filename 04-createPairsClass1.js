var featuresA;
var featuresB;
var alsoViewed;
var boughtTogether;
var totalItems = db.items.count();
var batchSize = 1000;
var numberBatches = Math.ceil(totalItems / batchSize);
var processedCount = 0;
var pairsCount = 0;
var boughtCount = 0;
var viewedCount = 0;
var pair;
var start;
var stop;
var currentBatch;
var itemIds;
var pairId;
var id;
var lastId = ''
var existed;
var exists;

function makeId(idA, idB) {
    if(idA < idB) {
        return idA.concat(idB);
    }
    else {
        return idB.concat(idA);
    }
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
    if(processedCount === 1 || processedCount % 100 === 0 || processedCount === totalItems) {
        print('(' + processedCount * 100 / totalItems + '%) - ' + processedCount + ' itens processados e ' + pairsCount + ' pares inseridos' + getTime());
    }
    return;
}

function processItem(item){
    id = item._id;
    item.related = item.related || {};
    boughtTogether = item.related.bought_together || [];
    featuresA = null;
    
    // Classe 1 - Bought Together
    boughtTogether.forEach(function(btid) {
        // Se já foi verificado, desconsiderar
        pairId = makeId(id, btid);
        exists = db.pairs.count({ _id: pairId });
        if(!exists) {
            featuresA = featuresA || db.features.findOne({_id: id}, { _id: 0, features: 1 }).features;
            featuresB = db.features.findOne({_id: btid}, { _id: 0, features: 1 }).features;
            pair = makePair(pairId, featuresA, featuresB, 1);
            return insertPair(pair);
        }
    });
    
    processedCount += 1
    return showProgress();
}

function begin() {
    try {
        start = new Date().getTime();
        currentBatch = 1;
        while(pairsCount < 516000) {
            db.items.find({ _id: { $gt: lastId } }).limit(batchSize).forEach(function(item) {
                lastId = item._id;
                processItem(item);
            });
            print('Batch ' + currentBatch + ' executada. lastId = ' + lastId + getTime());
            currentBatch += 1;
        }
        
        print('Todos os pares inseridos com sucesso' + getTime());
    }
    catch(error) {
        print('ERRO com ' + processedCount + ' itens processados, ' + pairsCount + ' pares processados. Na batch ' + currentBatch + '.' + getTime());
        print (error);
    }
}

