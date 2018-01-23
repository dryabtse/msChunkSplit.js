// ===============
// msChunkSplit.js 
// ===============
//
// The purpose of this script is to:
//   - identify all of the chunks for the given namespace
//   - calculate chunk sizes for all of the given namespace 
//       - there are three different modes of chunk size calculation
//          1) OPTIMIZE_CHUNK_SIZE_CALCULATION = false - in that case all of the chunk documents are read (slow, but accurate)
//          2) OPTIMIZE_CHUNK_SIZE_CALCULATION = true and DOUBLECHECK_CHUNK_SIZE = true - chunk size calculation is performed 
//             with the average document size. However for each chunk that exceeds the threshold we calculate the true 
//             size (should genarally be faster than (1), but still reasonably accurate)
//          3) OPTIMIZE_CHUNK_SIZE_CALCULATION = true and DOUBLECHECK_CHUNK_SIZE = false - similar to (2), except that we don't
//             calculate the true size for the qualifying chunks (the fastest and least accurate option)
//   - for qualifying chunks (those which size is equal or exceeds SPLIT_THRESHOLD) split points are calculated
//   - for those chunks that have one or more split point, splits will be executed
//   - lastly the script will count the number of chunks for the collection again - to show the result of splits
//
// Arguments:
//   NS - String, namespace for the sharded collection in the "database.collection" format
//   DO_SPLIT - Boolean, if set to false (default) all of the calculations will be performed except the actual chunk splits
//
// Prerquisites:
//   - it is expected that there are no writes happenning to the target namespace when the script is executed
//   - the Balancer should be stopped and disabled
//   - if authentication is enabled, the user running the script must have `internal` action granted for the `cluster` resource on the shards
//      db.createRole({role:"splitchunk", privileges:[{resource:{cluster:true}, actions:["internal"]}], roles:[]});
//      db.grantRolesToUser("admin", ["splitchunk"]);
//   - tested with `root` & `splitchunk` roles thus `root` role is recommended
//   - the script tested on v3.4 _only_. Use it with MongoDB v3.6 on your own risk
//
// Sample usage:
//   splitCollectionChunks("test.s3", false);
//   splitCollectionChunks("test.s5", true);

var splitCollectionChunks = function(NS, DO_SPLIT=false) {

    // Configurable globals start here

    var SAMPLE = 1; // Defines a portion of chunks to process - 1 == 100%
    var OPTIMIZE_CHUNK_SIZE_CALCULATION = true; // use avg doc size to calculate chunk sizes
    var DOUBLECHECK_CHUNK_SIZE = false; // in optimized mode, if split threshold is exceeded check the actual chunk size
    var SPLIT_THRESHOLD_RATIO = 0.9; // Split threshold

    // Modify the following statement as appropriate to add authentication credentials

    var AUTH_DB = "admin";
    var AUTH_CRED = { user: "admin", pwd: "123" };
    
    // Configurable globals end here

    // Get the maxumum chunk size configured for the cluster

    var getChunkSize = function() {
        if(db.getSiblingDB("config").settings.count({_id: "chunksize"}) != 0){
            var res = db.getSiblingDB("config").settings.findOne({_id: "chunksize"}, {_id:0, value:1});
            assert(res.value, "value field is not present");
            assert.lte(0, res.value, "maximum chunksize value is less than or equal to 0");
            return res.value * 1024 * 1024; 
        }
        return 64 * 1024 * 1024;
    };
    
    // Get the CSRS connection string

    var getCsrsUri = function() {
        var res = db.serverStatus()
        assert(res.sharding.configsvrConnectionString, "sharding.configsvrConnectionString field is not present");
        return res.sharding.configsvrConnectionString;
    };

    var CONFIGSVR = getCsrsUri(); // URI for config servers; DO NOT MODIFY
    var MAX_CHUNK_SIZE = getChunkSize(); // Maximum chunk size configured; DO NOT MODIFY
    var SPLIT_THRESHOLD = MAX_CHUNK_SIZE * SPLIT_THRESHOLD_RATIO; // Split threshold

    var CON_MAP = new Map();    

    db.getSiblingDB("config").shards.find({}).forEach(function(d) {
        var con = new Mongo(d.host);
        var res = con.getDB(AUTH_DB).auth(AUTH_CRED);
        assert.eq(1, res, "Authentication failed");
        CON_MAP.put(d._id, con);
    });

    var CHUNKS = [];
    var CHUNKS_TO_SPLIT = [];

    // Loop through the sharded collections and compose a list of arguments for the "datasize" command
    // in order to calculate the chunk sizes

    var getDatasizeArgs = function(namespace, percentage=SAMPLE) {
        var nsDoc = db.getSiblingDB("config").collections.findOne({_id: namespace}, {_id:1, key:1});
        assert(nsDoc._id, "The _id field is not present");
        assert(nsDoc.key, "The key field is not present");
        
        var count = db.getSiblingDB("config").chunks.count({ ns: nsDoc._id });
        assert.lt(0, count, "The number of chunks to process is less than 1");

        var sampleValue = count * percentage;
        if(sampleValue < 1) sampleValue = 1;

        db.getSiblingDB("config").chunks.aggregate([ {"$match": { ns: nsDoc._id }}, { $sample: { size: sampleValue } } ]).forEach(function(doc) {
            var newDoc = {
                "datasize": doc.ns,
                "keyPattern": nsDoc.key,
                "min": doc.min,
                "max": doc.max,
                "estimate": OPTIMIZE_CHUNK_SIZE_CALCULATION,
                "shard": doc.shard
            };
            CHUNKS.push(newDoc);
        });
    };

    // Calculate the maximum chunk size and check if the split threshold is exceeded

    var checkChunkSize = function(chunk) {
        var res = CON_MAP.get(chunk.shard).getDB("admin").runCommand(chunk);
        assert(res.size, "The size field is not present it the response");
        var chunkSize = res.size;

        if (chunkSize >= SPLIT_THRESHOLD) {
            if( (OPTIMIZE_CHUNK_SIZE_CALCULATION == true) && (DOUBLECHECK_CHUNK_SIZE == true) ) {
                chunk.estimate = false;
                var res = CON_MAP.get(chunk.shard).getDB("admin").runCommand(chunk);
                assert(res.size, "The size field is not present it the response");
                if (chunkSize > SPLIT_THRESHOLD) {
                    var chunkSize = res.size;
                }
                else chunkSize = -1;
            };
            return chunkSize;
        }
        else return -1;
    };

    // Obtain the split vector

    var getSplitVector = function(chunk) {
        var arguments = {
            "splitVector": chunk.datasize,
            "keyPattern": chunk.keyPattern,
            "min": chunk.min,
            "max": chunk.max,
            "maxChunkSizeBytes": MAX_CHUNK_SIZE
        };

        var res = CON_MAP.get(chunk.shard).getDB("admin").runCommand(arguments);
        assert(res.splitKeys, "The splitKey field is not present");

        if (res.splitKeys.length > 0) {
            chunk.canSplit = true;
            chunk.splitVector = res.splitKeys;
        };
    };

    // Get shard version for a chunk

    var getShardVersion = function(namespace) {
        var res = db.getSiblingDB("admin").runCommand({getShardVersion: namespace});
        assert(res.ok, "The ok field is not present");
        assert(res.version, "The version field is not present");
        assert(res.versionEpoch, "The versionEpoch field is not present");
        assert.eq(1, res.ok, "Failed to obtain the shard version");

        return [ res.version, res.versionEpoch ];
    }

    // Split a chunk

    var splitChunk = function(chunk, shardConnection, splitVector, shardVersion, configDB) {
        var args = {
            "splitChunk": chunk.datasize,
            "from": chunk.shard,
            "min": chunk.min,
            "max": chunk.max,
            "keyPattern": chunk.keyPattern,
            "splitKeys": splitVector,
            "configdb": configDB,
            "shardVersion": shardVersion
        };
  
        var res = shardConnection.getDB("admin").runCommand(args);
        assert(res.ok, "The ok field is not present");

        if(res.ok != 1){
            print("\nERROR: Chunk split failed");
            printjson(args);
            print("Reason:");
            printjson(res);
        };
    };

    // A few small helpers

    var getDbFromNs = function(namespace) {
        return namespace.split(".")[0];
    };

    var getColFromNs = function(namespace) {
        var arr = namespace.split(".");
        arr.shift();
        return arr.join(".");
    };

    var getChunkCounts = function(chunkArray) {
        assert.lt(0, chunkArray.length, "no chunks found" );
        assert(chunkArray[0].datasize, "datasize field is not present");
        print("Found " + chunkArray.length + " chunks for " + chunkArray[0].datasize + " collection\n");

        return chunkArray.length;
    };

    var getSplitChunkCounts = function(chunkArray) {
        var counter = 0;
        var resChunks = 0;
        chunkArray.forEach(function(c) {
            if(c.canSplit == true) {
                counter++;
                resChunks = resChunks + c.splitVector.length + 1;
            };
        });
        print("Identified " + counter + " chunks that can be split into " + resChunks + "\n");

        return counter;
    };

    var getCouldSplitChunkCounts = function(chunkArray) {
        var counter = 0;
        chunkArray.forEach(function(c) {
            if(c.chunkSize != -1) {
                counter++;
            };
        });
        print("Identified " + counter + " chunks that are candidates for splitting\n");

        return counter;
    };

    var printProgress = function(total, curPos, lastPosPrinted) {
        var numBucketsTotal = 20;
        var bucketString = "";
        var progressString = "";
        for(var i = 0; i < 100/numBucketsTotal; i++) {
            bucketString = bucketString + "#";
            progressString = progressString + "-";
        };
        var bucketSize = total/numBucketsTotal;
        var numBuckets = curPos/bucketSize;
        
        var delta = curPos - lastPosPrinted;
        
        if ((delta >= bucketSize) || (curPos == 0)) {
            var outLine = "[";
            for(var i = 1; i <= numBucketsTotal; i++) {
                if(i <= numBuckets) {
                    outLine = outLine + bucketString;
                }
                else
                    outLine = outLine + progressString;
            };
          outLine = outLine + "]" + " ... " + parseInt(numBuckets/numBucketsTotal*100) + "%";
          print(outLine);
          return curPos;
        };
        
        return lastPosPrinted;
      };

    /// MAIN SECTION

    // Step 1: Get the sample chunks

    print("\nStep 1: Looking for chunks for the " + NS + " collection..." );
    getDatasizeArgs(NS);
    var wasChunks = getChunkCounts(CHUNKS);


    // Step 2: Filter out the qualifying chunks

    print("Step 2: Filtering out potential split candidates...");
    var pos = 0;
    var i = 0;
    CHUNKS.forEach(function(c){ 
        var chunkSize = checkChunkSize(c);
        if(chunkSize > 0) {
            var chunkToSplit = c;
            chunkToSplit.chunkSize = chunkSize;
            chunkToSplit.canSplit = false;
            CHUNKS_TO_SPLIT.push(chunkToSplit);      
        };
        pos = printProgress(CHUNKS.length, i, pos);
        i++;
    });
    
    var couldSplitCount = getCouldSplitChunkCounts(CHUNKS_TO_SPLIT);
    if(couldSplitCount == 0) {
        print("There are no chunks to split. Aborting ...");
    }
    else {

        // Step 3: Obtain split points

        print("Step 3: Looking for split points for the candidate chunks...");
        var pos = 0;
        var i = 0;
        CHUNKS_TO_SPLIT.forEach(function(c) {
            getSplitVector(c);
            pos = printProgress(couldSplitCount, i, pos);
            i++;
        });

        var canSplitCount = getSplitChunkCounts(CHUNKS_TO_SPLIT);
        if(canSplitCount == 0) {
            print("There are no chunks to split. Aborting ...");
        }
        else {

            // Step 4: Split the qualifying chunks

            if (DO_SPLIT == true) {
                print("Step 4: Splitting the qualifying chunks...");
                var pos = 0;
                var i = 0;
                CHUNKS_TO_SPLIT.forEach(function(c) {
                    if(c.canSplit == true) {
                        var shardVersion = getShardVersion(c.datasize);
                        splitChunk(c, CON_MAP.get(c.shard), c.splitVector, shardVersion, CONFIGSVR);
                    };
                    pos = printProgress(canSplitCount, i, pos);
                    i++;
                });

                // Step 5: Let's validate the splits outcome

                print("\nStep 5: Checking if the number of chunks has changed...");
                CHUNKS = [];
                getDatasizeArgs(NS);
                var nowChunks = getChunkCounts(CHUNKS);
                print("There were " + (nowChunks - wasChunks) + " chunks added");
            }
            else {
                print("Splits were not requested. Exiting...");
            };
        };
    };

    return;
};