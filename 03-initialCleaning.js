var start;

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

start = new Date().getTime();
// Remove os itens sem características
var itemIds = db.itemsFull.find({}, { _id: 1 }).hint({ _id: 1 }).toArray();
var deletedCount = 0;
itemIds.forEach(function(item) {
    var _id = item._id;
    var isValid = db.features.count({_id: _id});
    if(!isValid) {
        deletedCount += 1;
        db.itemsFull.deleteOne({_id: _id});
        if(deletedCount % 1000 == 0) {
            print(deletedCount + ' itens removidos' + getTime());
        }
    }
});
print(deletedCount + ' itens removidos no total' + getTime());
itemIds = null;

start = new Date().getTime();
// Remove as referências inválidas
itemIds = db.itemsFull.find({}, { _id: 1 }).hint({ _id: 1 }).toArray();
var insertedCount = 0;
var checkedCount = 0;
itemIds.forEach(function(doc) {
    checkedCount += 1;
    var _id = doc._id;
    var itemFull = db.itemsFull.findOne({_id: _id}, {imUrl: 1, "related.also_viewed": 1, "related.bought_together": 1});
    var item = {
        _id: _id,
        imURL: itemFull.imUrl,
        related: {
            also_viewed: [],
            bought_together: []
        }
    };
    itemFull.related.also_viewed.forEach(function(avid) {
        var isValid = db.itemsFull.count({_id: avid});
        if(isValid) {
            item.related.also_viewed.push(avid);
        }
    });
    itemFull.related.bought_together.forEach(function(btid) {
        var isValid = db.itemsFull.count({_id: btid});
        if(isValid) {
            item.related.bought_together.push(btid);
        }
    });
    if(item.related.also_viewed.length > 0 || item.related.bought_together.length > 0) {
        insertedCount += 1;
        db.items.insertOne(item);
    }
    if(checkedCount || 1000 == 0 || checkedCount == itemIds.length) {
        print(checkedCount + ' itens verificados e ' + insertedCount + ' itens válidos' + getTime());
    }
});
itemIds = null;

start = new Date().getTime();
// Remove as características sem item
var featuresIds = db.features.find({}, { _id: 1 }).hint({ _id: 1 }).toArray();
deletedCount = 0;
featuresIds.forEach(function(features) {
    var _id = features._id;
    var isValid = db.items.count({_id: _id});
    if(!isValid) {
        deletedCount += 1;
        db.features.deleteOne({_id: _id});
        if(deletedCount % 1000 == 0) {
            print(deletedCount + ' características removidas' + getTime());
        }
    }
});
print(deletedCount + ' características removidas no total' + getTime());
featuresIds = null;

