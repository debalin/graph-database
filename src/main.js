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
        console.log("\nWriting proofs to " + egoCenter + ".proofs.");
        makeProof1.sync(null, egoCenter);
        console.log("Proofs written.");
        calculateMetrics(egoCenter);
        break;
    }
});

//prove or disprove the hypotheses
// Hypothesis 1 (czhao13) - People who have both same hometown and same university are more likely to have connection to each other.
function makeProof1(egoCenter, callback) {
    var query = "match (m), (n) where exists (m.`hometown;id`) and exists (n.`hometown;id`) and exists (m.`education;school;id`) and exists (n.`education;school;id`) and m.`hometown;id` = n.`hometown;id` and m.`education;school;id` = n.`education;school;id` return count(*)";
    db.cypherQuery(query, function(err, result) {
        fs.writeFileSync("../output/" + egoCenter + ".proofs", "Proof 1 (czhao13-01): \n");
        if (err) throw err;
        fs.appendFileSync("../output/" + egoCenter + ".proofs", "People having same hometown and same university: " + result.data + "\n");
        var denom = result.data;
        query = "match (m), (n) where exists (m.`hometown;id`) and exists (n.`hometown;id`) and exists (m.`education;school;id`) and exists (n.`education;school;id`) and m.`hometown;id` = n.`hometown;id` and m.`education;school;id` = n.`education;school;id` and (m)-[:FRIENDS]-(n) return count(*)";
        db.cypherQuery(query, function(err, result) {
            if (err) throw err;
            fs.appendFileSync("../output/" + egoCenter + ".proofs", "People having same hometown and same university and who are connected: " + result.data + "\n");
            var num = result.data;
            var percentage = (num / denom) * 100;
            fs.appendFileSync("../output/" + egoCenter + ".proofs", "Percentage of the latter: " + percentage + "\n");
            if (percentage > 50)
                fs.appendFileSync("../output/" + egoCenter + ".proofs", "Hypothesis 1 proved for this ego network." + "\n");
            else
                fs.appendFileSync("../output/" + egoCenter + ".proofs", "Hypothesis 1 disproved for this ego network." + "\n");
            if (callback)
                callback();
        });
    });
}

//calculate metrics
function calculateMetrics(egoCenter) {
    console.log("\nWriting metrics to " + egoCenter + ".metrics.");
    fs.writeFileSync("../output/" + egoCenter + ".metrics", "Metrics: \n");
    calculateNodeCount.sync(null, egoCenter);
    calculateEdgeCount.sync(null, egoCenter);
    calculateClusterCoefficient.sync(null, egoCenter);
    calculateBetweenness.sync(null, egoCenter);
    console.log("Metrics written.");
}

//calculate the numebr of nodes
function calculateNodeCount(egoCenter, callback) {
    var query = "start n=node(*) match (n) return count(n)";
    db.cypherQuery(query, function(err, result) {
        if (err) throw err;
        fs.appendFileSync("../output/" + egoCenter + ".metrics", "Node count: " + result.data + "\n");
        if (callback)
            callback();
    });
}

//calculate the number of edges 
function calculateEdgeCount(egoCenter, callback) {
    var query = "match (n:FBUser)-[r:FRIENDS]->(m:FBUser) return count(r)";
    db.cypherQuery(query, function(err, result) {
        if (err) throw err;
        fs.appendFileSync("../output/" + egoCenter + ".metrics", "Edge count: " + result.data + "\n");
        if (callback)
            callback();
    });
}

//calculate the cluster coefficients for all nodes
function calculateClusterCoefficient(egoCenter, callback) {
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
                var denominator = Combinatorics.C(n, 2);
                fs.appendFileSync("../output/" + egoCenter + ".metrics", "Clustering coefficient for " + name + ": " + (r / denominator) + "\n");
                if (callback)
                    callback();
            }
        });
    }
}

//calculate the betweenness centrality value for each node
function calculateBetweenness(egoCenter, callback) {
    var query = "start n=node(*) where exists (n.name) and n.name <> " + egoCenter + " with collect(n.name) as all_nodes start source=node(*), destination=node(*) match p = allShortestPaths((source)-[r:FRIENDS*]-(destination)) where source <> destination and length(p)> 1 and source.name <> " + egoCenter + " and destination.name <>" + egoCenter + " with extract(n IN NODES(p) | n.name) as nodes, all_nodes with collect(nodes) as paths, all_nodes return reduce(res=[], x in all_nodes | res + [x, length(filter(p in paths where x in tail(p) and x <> last(p)))])";
    db.cypherQuery(query, function(err, result) {
        if (err) throw err;
        var count = 0;
        for (var value of result.data[0]) {
        	if (count % 2 == 0) {
        		fs.appendFileSync("../output/" + egoCenter + ".metrics", "Centrality value for " + value + ": ");		
        	}
        	else {
        		fs.appendFileSync("../output/" + egoCenter + ".metrics", value + "\n");			
        	}
        	count++;
        }
        if (callback)
            callback();
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
    db.cypherQuery("match (n) detach delete (n)", function(err, results) {
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
    var query = "match (n:FBUser {name: '" + nodeName1 + "'})-[r:FRIENDS]-(m:FBUser {name: '" + nodeName2 + "'}) return sign(count(r))";
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
    updateWithCircles(egoCenter);
    console.log("Ego network built in Neo4j for " + egoCenter + ".");
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
