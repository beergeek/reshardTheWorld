/*
Use cases:
* Current sharded collection with incorrect shard key
* Current sharded collection with incorrect share key and incorrect unique setting
* Current sharded collection with correct shard key and incorrect unique setting
* Current sharded collection with incorrect shard key and correct unique setting 
* Current sharded collection that should not be sharded
* Current sharded collection that should be sharded
* Current sharded collection that should be sharded with a non-_id shard key
* Current sharded collection that should be sharded with a non-_id shard key and incorrect unique setting
* Current sharded collection that should be sharded with a non-_id shard key and correct unique setting
* Current sharded collection that should be sharded with a non-_id shard key and incorrect unique setting and forceRedistribution
* Current sharded collection that should be sharded with a non-_id shard key and correct unique setting and forceRedistribution
* Current sharded collection that is empty with correct shard key
* Current sharded collection that is empty and should be sharded

*/
sh.getBalancerState()
sh.stopBalancer()
sh.getBalancerState()
db.getSiblingDB("beers").createCollection("goodBeers");
db.getSiblingDB("beers").createCollection("goodLagers");
sh.enableSharding("beers");
sh.shardCollection("beers.goodBeers", { "name": 1 });
db.getSiblingDB("beers").createCollection("megaBeers");
db.getSiblingDB("beers").createCollection("mainStreamBeers");
sh.enableSharding("beers");
sh.shardCollection("beers.mainStreamBeers", { "_id": 1 }, { unique: true });
sh.shardCollection("beers.megaBeers", { "_id": 1 });
sh.shardCollection("beers.goodBeers", { "name": 1 });
sh.shardCollection("beers.goodLagers", { "_id": 1 });