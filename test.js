db.getSiblingDB("beers").createCollection("goodBeers");
sh.enableSharding("beers");
sh.shardCollection("beers.goodBeers", { "name": 1 });
db.getSibilingDB("beers").getCollection("goodBeers").insertMany("funny_beer_list_detailed.json");
