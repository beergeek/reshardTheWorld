const assert = require('assert');
function reshardCollection(ns, key, unique, forceRedistribution, numInitialChunks = 90, count = 0) {
  if (count > 5) {
    return false;
  }
  try {
    var result = db.adminCommand({ reshardCollection: ns, key: key, unique: unique, forceRedistribution: forceRedistribution, numInitialChunks: numInitialChunks });
    return true;
  } catch(e) {
    if (e.errorResponse.code === 4952606) {
      print("Collection needs less than "+numInitialChunks+" chunks to be resharded");
      print(e.errorResponse.errmsg);
      var exploded_msg = e.errorResponse.errmsg.split(" ");
      var initialChunks = parseInt(exploded_msg[exploded_msg.length - 2]);
      print(initialChunks);
      if (initialChunks == 1) {
        return true;
      }
      reshardCollection(ns, key, unique, forceRedistribution, initialChunks, count + 1);
    } else if (e.errorResponse.code === 338) {
      print("Sharding operations already in progress, exiting this script")
      return false;
    } else {
      print("Error: " + e.errorResponse.errmsg);
      return false;
    }
  }
}

var collection_setup = [
  {
    "name": "dev_beers",
    "collections": [
      {
        "name": "goodBeers",
        "shared": true,
        "shardKey": { "brewery": 1, "name": 1 },
        "unique": true,
      },
      {
        "name": "goodLagers",
        "shared": true,
        "shardKey": { "brewery": 1, "name": 1 },
        "unique": true,
      },
      {
        "name": "mainStreamBeers",
        "shared": true,
        "shardKey": { "_id": 1 },
        "unique": false
      },
      {
        "name": "megaBeers",
        "shared": false
      },
      {
        "name": "magaLagers",
        "shared": false,
      },
      {
        "name": "logs",
        "shared": true,
        "shardKey": { "_id": 1 },
        "unique": false
      }
    ]
  }
]
var database=collection_setup[0].name;
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
      print(database+"."+collection+" is sharded");

      var currentShardKey = null;
      var currentUnique = false;
      var requiredShardKey = null;
      var requiredUnique = false;
      var forceRedistribution = false;

      // Check if we should be unsharded
      var described = false;
      collection_setup[0].collections.forEach(function(coll) {
        if (coll.name == collection) {
          if (coll.shared == false) {
            print("Collection is not supposed to be sharded, unsharding");
            sh.unshardCollection(database+"."+collection, "rs1");
            return;
          } else if (coll.shared == true) {
            print("Collection is supposed to be sharded");
            requiredShardKey = coll.shardKey;
            if (typeof coll.unique === "undefined") {
              requiredUnique = false;
            } else {
              requiredUnique = coll.unique;
            }
            described = true;
            shared = true;
          } else {
            print("Collection is not registered as sharded or unsharded, skipping");
            return;
          }
        }
      })
      if (described == false) {
        print(database+"."+collection+" collection is not registered");
        return;
      }

      // test to see if we have enough documents to redistribute
      if (stats.count < 1000) {
        print("Collection has less than 1000 documents, not redistributing");
        forceRedistribution = false;
      } else {
        shardDist.forEach(function(coll) {
          if (coll.ns == database + "." + collection) {
            print("Collection is on " + coll.shards.length + " shards");
            if (coll.shards.length < shardCount) {
              forceRedistribution = true;
            }
          }
        })
      }

      // get shard key and unique index settings
      configDB.collections.find({}).forEach(function(ns) {
        if (ns._id == database + "." + collection) {
          currentShardKey = ns.key;
          if (ns.unique == null) {
            currentUnique = false;
          } else if (ns.unique == true) {
            currentUnique = true;
          } else {
            currentUnique = false;
          }
        }
      });
      print("Current Shard Key: " + JSON.stringify(currentShardKey));
      print("Current Unique Value: " + currentUnique);
      print("Required Shard Key: " + JSON.stringify(requiredShardKey));
      print("Required Unique Value: " + requiredUnique);
      var reIndex = false;

      // Check if the collection should be using the right shard key
      if (requiredShardKey === currentShardKey && requiredUnique === currentUnique && forceRedistribution == false) {
        print("Collection is using the correct shard key and unique setting, and does not need to be redistributed");
        return;
      }
      try {
        if (assert.deepStrictEqual(JSON.parse(currentShardKey), JSON.parse(requiredShardKey))) {
          print("Collection is using the correct shard key");
        }
      } catch (e) {
        if (e instanceof assert.AssertionError) {
          print("Collection is not using the correct shard key"+ JSON.stringify(e));
          reIndex = true;
        }
      }
      try {
        if (assert.deepStrictEqual(JSON.parse(currentUnique), JSON.parse(requiredUnique))) {
          print("Collection is using the correct unique index setting");
        }
      } catch (e) {
        if (e instanceof assert.AssertionError) {
          print("Collection is not using the correct unique index setting");
          reIndex = true;
        }
      }
      if (reIndex == true) {
        print("About to reshard "+database+"."+collection+" with shard key "+JSON.stringify(requiredShardKey)+" and unique indexes set to "+requiredUnique+" and forceRedistribution set to "+forceRedistribution);
        var tempIndex = false;
        try {
          if (requiredShardKey._id === 1) {
            print("Not creating index as using default _id index");
            requiredUnique = false;
          } else {
            print("Creating index on shard key: " + JSON.stringify(requiredShardKey)+" with unique set to " + requiredUnique);
            var res = context.getCollection(collection).createIndex(requiredShardKey, { unique: requiredUnique, name: "shardKeyIndex" });
            print("Index created: " + JSON.stringify(res));
          }
        } catch (e) {
          if (/An existing index has the same name as the requested index/.test(e)) {
            print("Index already exists, not creating");
          } else {
            print("Error creating index: " + e.errorResponse.errmsg);
          }
        }
        var success = reshardCollection(database+"."+collection, requiredShardKey, requiredUnique, forceRedistribution);
        if (success) {
          print("Collection is sharded correctly");
          if (tempIndex == true) {
            print("Dropping temporary index");
            context.collection.dropIndex("tempIndex");
          }
        } else {
          print("Collection is not sharded correctly");
        }
      }
    } else {
      print(database+"."+collection+" is not sharded");
      var described = false;
      collection_setup[0].collections.forEach(function(coll) {
        if (coll.name == collection) {
          if (coll.sharded == false) {
            print("Collection is supposed to be unsharded, skipping");
            return;
          } else if (coll.shared == true) {
            print("Collection is supposed to be sharded");
            requiredShardKey = coll.shardKey;
            requiredUnique = coll.unique;
            described = true;
            shared = true;
            try {
              print("Creating index on shard key: " + JSON.stringify(requiredShardKey));
              if (requiredShardKey._id === 1) {
                print("Not creating index as using default _id index");
              } else {
                print("Creating index on shard key: " + JSON.stringify(requiredShardKey));
                var res = context.collection.createIndex(requiredShardKey, { unique: requiredUnique });
                print("Index created: " + JSON.stringify(res));
              }
            } catch (e) {
              if (/An existing index has the same name as the requested index/.test(e)) {
                print("Index already exists, not creating");
              } else {
                print("Error creating index: " + e);
              }
            }
            try {
              print("Sharing collection");
              sh.shardAndDistributeCollection(database+"."+collection, nonIDKey);
            } catch (e) {
              print(e.errorRsponse.errmsg);
            }
          } else {
            print("Collection is not registered as sharded or unsharded, skipping");
            return;
          }
        }
      })
      if (described == false) {
        print(database+"."+collection+" collection is not registered");
        return;
      }
    }

})

print("\n==========Completing ==========================================================\n");
var bs = sh.setBalancerState(true);
balancerState = sh.startBalancer();
if (balancerState) {
  print("Balancer is running");
} else {
  print("Balancer is not running");
}

    