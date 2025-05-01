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

* goodLagers should be sharded by _id but is sharded by name
*/
sh.getBalancerState()
sh.stopBalancer()
sh.getBalancerState()
db.getSiblingDB("dev_beers").createCollection("goodBeers");
db.getSiblingDB("dev_beers").createCollection("goodLagers");
sh.enableSharding("dev_beers");
sh.shardCollection("dev_beers.goodBeers", { "name": 1 });
db.getSiblingDB("dev_beers").createCollection("megaBeers");
db.getSiblingDB("dev_beers").createCollection("mainStreamBeers");
db.getSiblingDB("dev_beers").createCollection("logos");
db.getSiblingDB("dev_beers").createCollection("comments");
sh.shardCollection("dev_beers.mainStreamBeers", { "_id": 1 }, { unique: true });
sh.shardCollection("dev_beers.megaBeers", { "_id": 1 });
sh.shardCollection("dev_beers.goodBeers", { "name": 1 });
sh.shardCollection("dev_beers.goodLagers", { "_id": 1 });
load("amazing_beer_list_with_breweries.js");
load("boring_beer_list_with_breweries.js");
load("funny_beer_list_with_breweries.js");