/*
NOTE: You cannot reshard a collection from one unique shard key or non-unique shard key to unique shard key. This requires a manual intervention
to unshard and then shard the collection again letting the balancer do the work, not resharding.
*/
const assert = require('assert');

function reshardCollection(ns, key, unique, forceRedistribution, numInitialChunks = 90) {
  try {
    db.adminCommand({ reshardCollection: ns, key: key, unique: unique, forceRedistribution: forceRedistribution, numInitialChunks: numInitialChunks });
    return true;
  } catch (e) {
    if (e.errorResponse.code === 4952606) {
      print("Collection needs less than " + numInitialChunks + " chunks to be resharded");
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

function getStats(context, collection) {
  var stats = context.getCollection(collection).stats();
  return stats;
}
  

var collection_setup = [
  {
    "name": "dev_beers",
    "collections": [
      {
        "name": "goodBeers",
        "sharded": true,
        "shardKey": { "brewery": 1, "name": 1 },
        "unique": true,
      },
      {
        "name": "goodLagers",
        "sharded": true,
        "shardKey": { "brewery": 1, "name": 1 },
        "unique": true,
      },
      {
        "name": "mainStreamBeers",
        "sharded": true,
        "shardKey": { "_id": 1 },
        "unique": true
      },
      {
        "name": "megaBeers",
        "sharded": false
      },
      {
        "name": "magaLagers",
        "sharded": false,
      },
      {
        "name": "logs",
        "sharded": true,
        "shardKey": { "_id": 1 },
        "unique": false
      }
    ]
  }
]


// Stop the balancer for resharding operations
sh.stopBalancer();
sh.setBalancerState(false);
var balancerState = sh.getBalancerState();
if (balancerState) {
  print("Balancer is running, even after attempting to stop, aborting");
  exit(1);
}
var shard_details = sh.listShards();
var shardCount = shard_details.length;
var shardNames = [];
// Get the shard names
shard_details.forEach(function (shard) {
  shardNames.push(shard._id);
});
print("Shard Count: " + shardCount);
var shardDist = sh.getShardedDataDistribution();
var configDB = db.getSiblingDB("config");

// count for unsharding collections
var count = 0;
collection_setup.forEach(function (database_obj) {
  var database = database_obj.name;

  context = db.getSiblingDB(database);

  // Iterate over the collections in the database
  context.getCollectionNames().forEach(function (collection) {

    print("\n" + database + "." + collection + "\n=========================================================");

    var stats = getStats(context, collection);
    // Check if the collection is sharded
    if (stats.sharded) {
      print(database + "." + collection + " is sharded");

      var currentShardKey = null;
      var currentUnique = false;
      var requiredShardKey = null;
      var requiredUnique = false;
      var forceRedistribution = false;

      // Check if we should be unsharded
      var described = false;
      // Check if the collection is supposed to be sharded or unsharded, and get the shard key and unique index settings
      for (var i = 0; i < database_obj.collections.length; i++) {
        var coll = database_obj.collections[i];
        if (coll.name == collection) {
          described = true;
          if (coll.sharded == false) {
            var recipient_shard = shardNames[count % shardCount];
            print("Collection is not supposed to be sharded, unsharding to "+ recipient_shard);
            sh.unshardCollection(database + "." + collection, recipient_shard);
            count += 1;
          } else if (coll.sharded == true) {
            print("Collection is supposed to be sharded");
            requiredShardKey = coll.shardKey;
            if (typeof coll.unique === "undefined") {
              requiredUnique = false;
            } else {
              requiredUnique = coll.unique;
            }
            sharded = true;
          } else {
            print("Collection is not registered as sharded or unsharded, skipping");
          }
          break;
        }
      }
      if (described == false) {
        print(database + "." + collection + " collection is not registered");
        return;
      }

      // test to see if we have enough documents to redistribute
      if (stats.count < 1000) {
        print("Collection has less than 1000 documents, not redistributing");
        forceRedistribution = false;
      } else {
        for (var i = 0; i < shardDist.length; i++) {
          var coll = shardDist[i];
          if (coll.ns == database + "." + collection) {
            print("Collection is on " + coll.shards.length + " shards");
            if (coll.shards.length < shardCount) {
              forceRedistribution = true;
            }
            break;
          } 
        }
      }

      // get current shard key and unique index settings
      var shard_details = configDB.shards.find({}).toArray();
      for (var i = 0; i < shard_details.length; i++) {
        var ns = shard_details[i];
        if (ns._id == database + "." + collection) {
          currentShardKey = ns.key;
          if (ns.unique == null) {
            currentUnique = false;
          } else if (ns.unique == true) {
            currentUnique = true;
          } else {
            currentUnique = false;
          }
          break;
        }
      }
      print("Current Shard Key: " + JSON.stringify(currentShardKey));
      print("Current Unique Value: " + currentUnique);
      print("Required Shard Key: " + JSON.stringify(requiredShardKey));
      print("Required Unique Value: " + requiredUnique);
      var reIndex = false;

      // Check if the collection should be using the right shard key
      try {
        if (assert.deepStrictEqual(JSON.stringify(currentShardKey), JSON.stringify(requiredShardKey))) {
          print("Collection is using the correct shard key");
        }
      } catch (e) {
        if (e instanceof assert.AssertionError) {
          print("Collection is not using the correct shard key" + JSON.stringify(e));
          reIndex = true;
        }
      }
      // Check if the collection should be using the right unique index setting
      if (currentUnique !== requiredUnique) {
        print("Collection is not using the correct unique index setting");
        if (requiredUnique == true) {
          print("We cannot transition from one unique index to another unique index or from a non-unique index to a unique index, this requires manual intervention");
          return;
        }
      }
      // If we need to reshard go ahead and create the index and then reshard
      if (reIndex == true) {
        print("About to reshard " + database + "." + collection + " with shard key " + JSON.stringify(requiredShardKey) + " and unique indexes set to false and forceRedistribution set to " + forceRedistribution);
        try {
          if (requiredShardKey._id === 1) {
            print("Not creating index as using default _id index");
          } else {
            print("Creating index on shard key: " + JSON.stringify(requiredShardKey) + " with unique set to false");
            var res = context.getCollection(collection).createIndex(requiredShardKey);
            print("Index created: " + JSON.stringify(res));
          }
        } catch (e) {
          if (/An existing index has the same name as the requested index/.test(e)) {
            print("Index already exists, not creating");
          } else {
            print("Error creating index: " + e.errorResponse.errmsg);
          }
        }
        print("Starting resharding");
        var success = reshardCollection(database + "." + collection, requiredShardKey, false, forceRedistribution);
        if (success) {
          print("Collection is sharded correctly");
        } else {
          print("Collection is not sharded correctly");
        }
      } else {
        print("Collection is using the correct shard key and unique setting, and does not need to be redistributed");
      }
    } else {
      print(database + "." + collection + " is not sharded");
      var described = false;
      for (var i = 0; i < database_obj.collections.length; i++) {
        var coll = database_obj.collections[i];
        if (coll.name == collection) {
          described = true;
          if (coll.sharded == false) {
            print("Collection is supposed to be unsharded, skipping");
            return;
          } else if (coll.sharded == true) {
            print("Collection is supposed to be sharded");
            requiredShardKey = coll.shardKey;
            requiredUnique = coll.unique;
            sharded = true;
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
              sh.shardCollection(database + "." + collection, requiredShardKey);
              var success = reshardCollection(database + "." + collection, requiredShardKey, false, forceRedistribution);
              if (success) {
                print("Collection is sharded correctly");
              } else {
                print("Collection is not sharded correctly");
              }
            } catch (e) {
              print(e.errorRsponse.errmsg);
            }
          } else {
            print("Collection is not registered as sharded or unsharded, skipping");
          }
          break;
        }
      }
      if (described == false) {
        print(database + "." + collection + " collection is not registered");
      }
    }
  })
})

print("\n==========Completing ==========================================================\n");
var bs = sh.setBalancerState(true);
balancerState = sh.startBalancer();
if (balancerState) {
  print("Balancer is running");
} else {
  print("Balancer is not running");
}

