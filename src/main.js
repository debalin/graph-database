var neo4j = require("node-neo4j");
var fs = require("fs");
var sync = require("sync");
var Combinatorics = require('js-combinatorics');

var db = new neo4j('http://neo4j:password@localhost:7474');
var egoCenters = ["0", "107", "348", "414", "686", "698", "1684", "1912", "3437", "3980"];
var features = {};
var individualFeatures = {};
var egoFeatures = {};
var friends = {};
var currentNodeID = "";
var egoCenterID;
var tempResult = {};

sync(function() {
    for (var egoCenter of egoCenters) {
        deleteEverything.sync(null);
        createEgoNetwork(egoCenter);
        calculateMetrics(egoCenter);
    }
});

//calculate metrics
function calculateMetrics(egoCenter) {
    console.log("\nMetrics - ");
    calculateNodeCount();
    calculateEdgeCount();
    calculateClusterCoefficient();
    calculateBetweenness(egoCenter);
}

//calculate the numebr of nodes
function calculateNodeCount() {
    var query = "start n=node(*) match (n) return count(n)";
    db.cypherQuery(query, function(err, result) {
        if (err) throw err;
        console.log("Node count: " + result.data);
    });
}

//calculate the number of edges 
function calculateEdgeCount() {
    var query = "match (n:FBUser)-[r:FRIENDS]->(m:FBUser) return count(r)";
    db.cypherQuery(query, function(err, result) {
        if (err) throw err;
        console.log("Edge count: " + result.data);
    });
}

//calculate the cluster coefficients for all nodes
function calculateClusterCoefficient() {
    var query1 = "start a = node("
    var query2 = ") match (a)--(b) with a, b as neighbours match (a)--()-[r]-()--(a) where id(a) <> id(neighbours) and id(neighbours) <> 0 return count(distinct neighbours), count(distinct r), a.name";
    for (var friend in friends) {
        var query = query1 + friends[friend] + query2;
        db.cypherQuery(query, function(err, result) {
            if (err) throw err;
            for (var eachResult of result.data) {
                var n = eachResult[0];
                var r = eachResult[1];
                var name = eachResult[2];
                // var denominator = factorial(n).value[0] / factorial(n - 2).value[0];
                // var denominator = n * (Math.pow(2, (n - 1)) - 1);
                var denominator = Combinatorics.C(n, 2);
                console.log("Clustering coefficient for " + name + ": " + (r / denominator));
            }
        });
    }
}

//calculate the betweenness centrality value for each node
function calculateBetweenness(egoCenter) {
    var query = "START n=node(*) WHERE EXISTS (n.name) and n.name <> " + egoCenter + " WITH collect(n.name) AS all_nodes START source=node(*), destination=node(*) MATCH p = allShortestPaths((source)-[r:FRIENDS*]-(destination)) WHERE source <> destination AND LENGTH(p)> 1 AND source.name <> " + egoCenter + " AND destination.name <>" + egoCenter + " WITH EXTRACT(n IN NODES(p) | n.name) AS nodes, all_nodes WITH COLLECT(nodes) AS paths, all_nodes RETURN reduce(res=[], x IN all_nodes | res + [x, length(filter(p IN paths where x IN tail(p) AND x <> last(p)))])";
    db.cypherQuery(query, function(err, result) {
        if (err) throw err;
        console.log("Centrality value for all nodes: " + result.data);
    });
}

//helper function to create the whole ego network for a particular node
function createEgoNetwork(egoCenter) {
    console.log("Creating ego network for " + egoCenter + ".");
    createEgoCenter(egoCenter);
}

//create the ego center node along with its features
function createEgoCenter(egoCenter) {
    console.log(" - Creating ego center.");
    createFeatures(egoCenter);
    readIndividualFeatures(egoCenter);
    readEgoFeatures(egoCenter);
    createEgoNode(egoCenter);
}

//create features from node_id.featnames file
function createFeatures(egoCenter, callback) {
    console.log(" - - Creating features.");
    var data = fs.readFileSync("../data/" + egoCenter + ".featnames", 'utf8');
    var featureArray = data.split("\n");
    for (var feature of featureArray) {
        var spaceIndex = feature.indexOf(" ");
        if (spaceIndex > 0) {
            features[feature.substring(0, spaceIndex)] = feature.substring(spaceIndex + 1, feature.length);
        }
    }
}

//read node_id.feat to get all features for the nodes in the ego network
function readIndividualFeatures(egoCenter, callback) {
    console.log(" - - Reading individual features.");
    var data = fs.readFileSync("../data/" + egoCenter + ".feat", 'utf8');
    var featureArray = data.split("\n");
    for (var feature of featureArray) {
        var spaceIndex = feature.indexOf(" ");
        if (spaceIndex > 0) {
            individualFeatures[feature.substring(0, spaceIndex)] = feature.substring(spaceIndex + 1, feature.length).split(" ");
        }
    }
}

//read node_id.egofeat to get the ego features
function readEgoFeatures(egoCenter, callback) {
    console.log(" - - Reading ego features.");
    var data = fs.readFileSync("../data/" + egoCenter + ".egofeat", 'utf8');
    egoFeatures = data.split(" ");
}

//finally create the ego node
function createEgoNode(egoCenter) {
    console.log(" - - Creating ego node.");
    var userFeatures = readNodeFeatures(egoFeatures);
    userFeatures.name = egoCenter;
    createUser.sync(null, userFeatures, egoCenter);
    createFriendNodes(egoCenter);
}

//read node features 
function readNodeFeatures(presentFeatures) {
    var userFeatures = {};
    for (var i = 0; i < presentFeatures.length; i++) {
        if (presentFeatures[i] == '1') {
            var feature = features["" + i];
            var colonIndex = feature.lastIndexOf(";");
            var propName = feature.substring(0, colonIndex);
            var propValue = feature.substring(colonIndex + 1, feature.length);
            userFeatures[propName] = propValue;
        }
    }
    return userFeatures;
}

//delete everything in the Neo4j database
function deleteEverything(callback) {
    db.cypherQuery("MATCH (n) DETACH DELETE (n)", function(err, results) {
        if (err) throw err;
        console.log("\nDatabase deleted.");
        if (callback)
            callback();
    });
}

//create a single node in the Neo4j database
function createUser(userFeatures, egoCenter, callback) {
    db.insertNode(userFeatures, 'FBUser', function(err, node) {
        if (err) throw err;
        // console.log("User created with ID " + node._id + ".");
        if (egoCenter)
            egoCenterID = node._id;
        else
            currentNodeID = node._id;
        if (callback)
            callback();
    });
}

//create relationships between nodes
function createRelationship(nodeID1, nodeID2, nodeName1, nodeName2, callback) {
    var query = "MATCH (n:FBUser {name: '" + nodeName1 + "'})-[r:FRIENDS]-(m:FBUser {name: '" + nodeName2 + "'}) RETURN SIGN(COUNT(r))";
    db.cypherQuery(query, function(err, result) {
        // console.log(nodeID1 + " " + nodeID2 + " " + result.data[0] + " " + nodeName1 + " " + nodeName2);
        if (result.data[0] == 0) {
            db.insertRelationship(nodeID1, nodeID2, 'FRIENDS', {}, function(err, relationship) {
                if (err) throw err;
                if (callback)
                    callback();
            });
        } else {
            if (callback)
                callback();
        }
    });
}

//create friend nodes for the ego network
function createFriendNodes(egoCenter) {
    console.log(" - Creating friend nodes.");
    var data = fs.readFileSync("../data/" + egoCenter + ".edges", 'utf8');
    var edgeArray = data.split("\n");
    for (var edge of edgeArray) {
        var pair = edge.split(" ");
        var userFeatures = {};
        if (!friends.hasOwnProperty(pair[0]) && pair[0]) {
            userFeatures = readNodeFeatures(individualFeatures[pair[0]]);
            userFeatures.name = pair[0];
            createUser.sync(null, userFeatures, null);
            friends[pair[0]] = currentNodeID;
            createRelationship.sync(null, egoCenterID, friends[pair[0]], egoCenter, pair[0]);
        }
        if (!friends.hasOwnProperty(pair[1]) && pair[1]) {
            userFeatures = readNodeFeatures(individualFeatures[pair[1]]);
            userFeatures.name = pair[1];
            createUser.sync(null, userFeatures, null);
            friends[pair[1]] = currentNodeID;
            createRelationship.sync(null, egoCenterID, friends[pair[1]], egoCenter, pair[1]);
        }
        createRelationship.sync(null, friends[pair[0]], friends[pair[1]], pair[0], pair[1]);
    }
    console.log(" - Relationships formed among nodes.");
    // updateWithCircles(egoCenter);
}

//update node features
function updateNode(nodeID, update, callback) {
    db.readNode(nodeID, function(err, node) {
        if (err) throw err;
        node[update] = "yes";
        db.updateNode(nodeID, node, function(err, node) {
            if (err) throw err;
            if (callback)
                callback();
        });
    });
}

//read node_id.circles file and update nodes with circles
function updateWithCircles(egoCenter) {
    console.log(" - Forming circles.");
    var data = fs.readFileSync("../data/" + egoCenter + ".circles", 'utf8');
    var circles = data.split("\n");
    for (var circle of circles) {
        var circleName = circle.substring(0, circle.indexOf("\t"));
        var circleMembers = circle.substring(circle.indexOf("\t") + 1, circle.length).split("\t");
        for (circleMember of circleMembers) {
            if (friends[circleMember + ""])
                updateNode.sync(null, friends[circleMember], circleName);
        }
    }
}
