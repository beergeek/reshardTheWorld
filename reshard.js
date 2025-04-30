function reshardCollection(ns, key, unique, forceRedistribution, numInitialChunks = 90, count = 0) {
  if (count > 5) {
    return false;
  }
  try {
    var result = db.adminCommand({ shardCollection: ns, key: key, unique: unique, forceRedistribution: forceRedistribution, numInitialChunks: numInitialChunks });
    print(JSON.stringify(result));
    return true;
  } catch(e) {
    if (e.errorRsponse.code == 4952606) {
      print("Collection needs less than "+numInitialChunks+" chunks to be resharded");
      var initialChunks = e.errorRsponse.split(" ")[-2];
      print(initialChunks);
      reshardCollection(ns, key, unique, forceRedistribution, initialChunks, count + 1);
    }
  }
}
var unshardedList = [ "megaBeers" ];
var nonIDCollections = [ {"goodBeers": [{"name": 1}, true]}, {"goodLagers": [{"name": 1}, false]} ];
var database="beers";
var configDB = db.getSiblingDB("config");
 
context=db.getSiblingDB(database);
sh.stopBalancer();
sh.setBalancerState(false);
var balancerState = sh.getBalancerState();
if (balancerState) {
  print("Balancer is running, even after attempting to stop, aborting");
  exit(1);
}
var shardCount = sh.listShards().length;
print("Shard Count: " + shardCount);
var shardDist = sh.getShardedDataDistribution();
 
context.getCollectionNames().forEach(function(collection){

    print("\n"+database+"."+collection+"\n=========================================================");

    var stats = context.getCollection(collection).stats();
    if (stats.sharded) {
      print("Sharded");

      // Check if we should be unsharded
      if (unshardedList.indexOf(collection) > -1) {
        print("Collection is not supposed to be sharded, unsharding");
        sh.unshardCollection(database+"."+collection, "rs1");
        return;
      }

      var shardKey = null;
      var unique = false;
      var forceRedistribution = false;

      // test to see if we have enough documents to redistribute
      if (stats.count < 1000) {
        print("Collection has less than 1000 documents, not redistributing");
        forceRedistribution = false;
      } else {
        shardDist.forEach(function(coll) {
          if (coll.ns == database + "." + collection) {
            print("Collection has " + coll.shards.length + " shards");
            if (coll.shards.length < shardCount) {
              forceRedistribution = true;
            }
          }
        })
      }

      // get shard key and unique index settings
      configDB.collections.find({}).forEach(function(ns) {
        if (ns._id == database + "." + collection) {
          shardKey = ns.key;
          if (ns.unique == null) {
            unique = false;
          } else if (ns.unique == true) {
            unique = true;
          } else {
            unique = false;
          }
        }
      });
      print("Shard Key: " + JSON.stringify(shardKey));
      print("Unique: " + unique);

      // Check if the collection should be using a non-_id shard key and if this is correct for the shard
      var found = nonIDCollections.find(obj => Object.keys(obj)[0] === collection);
      print(JSON.stringify(found));
      if (found && shardKey != found[collection][0] && unique != found[collection][1]) {
        print("Collection is not sharded by "+ JSON.stringify(found[collection][0]) + " or the unique setting for the index is incorrect");
        shardKey = found[collection][0];
        if (found[collection][1] != null) {
          unique = true;
        }
      } else if (found && shardKey == found[collection][0] && unique == found[collection][1] && forceRedistribution == false) {
        // if this is true the shardkey is correct and the collection is too small to redistribute
        print("Collection shard key is correct and collection is too small to redistribute");
        return;
      } else if (found === "" && shardKey != {"_id":1} ) {
        print("Required shard key and options: "+found);
        //this means we should not be using a non-_id shard key and the shard key is not _id
        print("Collection is sharded but not by _id");
        shardKey = { "_id": 1 };
      } else if (forceRedistribution == false) {
        // if this is true the shardkey is correct and the collection is too small to redistribute
        print("Collection shard key is correct and collection is too small to redistribute");
        return;
      }
      print("About to reshard "+database+"."+collection+" with shard key "+JSON.stringify(shardKey)+" and unique "+unique+" and forceRedistribution "+forceRedistribution);
      try {
        if (shardKey == { "_id": 1 }) {
          print("Not creating index as using default _id index");
        } else {
          var res = context.collection.createIndex(shardKey, { unique: unique });
        }
        print("Index created: " + JSON.stringify(res));
      } catch (e) {
        print("Error creating index: " + e);
      }
      success = reshardCollection(database+"."+collection, shardKey, unique, forceRedistribution);
      if (success) {
        print("Collection is sharded correctly");
      } else {
        print("Collection is not sharded correctly");
      }
    } else {
      print("Not Sharded");
      if (unshardedList.indexOf(collection) == -1) {
        print("Collection is supposed to be sharded");
        if (nonIDCollections.indexOf(collection) > -1) {
          var nonIDKey = JSON.stringify(nonIDCollections[collection][0]);
          print("Sharding collection with non-_id key");
          sh.shardAndDistributeCollection(database+"."+collection, nonIDKey);
        } else {
          print("Sharding collection");
          sh.shardAndDistributeCollection(database+"."+collection, { "_id": 1 });
        }
      }
    }

})


sh.setBalancerState(true);
sh.startBalancer();

    