// Prerquisites:
//   - if authentication is enabled, the user running the script must have `internal` action granted for the `cluster` resource on the shards
//      db.createRole({role:"splitchunk", privileges:[{resource:{cluster:true}, actions:["internal"]}], roles:[]});
//      db.grantRolesToUser("admin", ["splitchunk"]);
//   - tested with `root` & `splitchunk` roles thus `root` role is recommended

(function(NS, DO_SPLIT=false) {

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

    // Configurable globals go here
    var SAMPLE = 1; // Defines a portion of chunks to process - 1 == 100%
    var OPTIMIZE_CHUNK_SIZE_CALCULATION = true; // use avg doc size to calculate chunk sizes
    var DOUBLECHECK_CHUNK_SIZE = false; // in optimized mode, if split threshold is exceeded check the actual chunk size
    var CONFIGSVR = getCsrsUri();
    var MAX_CHUNK_SIZE = getChunkSize();
    var SPLIT_THRESHOLD = MAX_CHUNK_SIZE * 0.2;

    var CON_MAP = new Map();
    // Sample usage CON_MAP.get("sh_0").getDB("test").s.find()
    // Modify the following statement as appropriate to add auth credentials
    var AUTH_DB = "admin";
    var AUTH_CRED = { user: "admin", pwd: "123" };

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

    var getDatasizeArgs2 = function(namespace, percentage=SAMPLE) {
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

    var checkChunkSize2 = function(chunk) {
        var res = CON_MAP.get(chunk.shard).getDB("admin").runCommand(chunk);
        assert(res.size, "The size field is not present it the response");
        var chunkSize = res.size;

        if (chunkSize > SPLIT_THRESHOLD) {
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
        var splitVector = CON_MAP.get(chunk.shard).getDB("admin").runCommand(arguments).splitKeys;
        if (splitVector.length > 0) {
            chunk.canSplit = true;
            chunk.splitVector = splitVector;
        };
    };

    // Get shard version for a chunk

    var getShardVersion = function(chunk) {
        var epoch = db.getSiblingDB("config").chunks.findOne({
            "ns": chunk.datasize,
            "min": chunk.min,
            "max": chunk.max
            }, {
                "_id": 0,
                "lastmod": 1,
                "lastmodEpoch": 1
            });
        return [ epoch.lastmod, epoch.lastmodEpoch ];
    };

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
        assert(res.ok, "The `ok` field is not present");

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
    }

    /// MAIN SECTION

    // Step 1: Get the sample chunks
    print("Step 1: Looking for chunks for the " + NS + " collection..." );
    getDatasizeArgs2(NS);
    getChunkCounts(CHUNKS);


    // Step 2: Filter out the qualifying chunks
    print("Step 2: Filtering out potential split candidates...");
    CHUNKS.forEach(function(c){ 
        var chunkSize = checkChunkSize2(c);
        if(chunkSize > 0) {
            var chunkToSplit = c;
            chunkToSplit.chunkSize = chunkSize;
            chunkToSplit.canSplit = false;
            CHUNKS_TO_SPLIT.push(chunkToSplit);      
        };
    });
    
    var couldSplitCount = getCouldSplitChunkCounts(CHUNKS_TO_SPLIT);
    if(couldSplitCount == 0) {
        print("There are no chunks to split. Aborting ...");
    }
    else {
        // Step 3: Obtain split points
        print("Step 3: Looking for split points for the candidate chunks...");
        CHUNKS_TO_SPLIT.forEach(function(c) {
            getSplitVector(c);
        });

        var canSplitCount = getSplitChunkCounts(CHUNKS_TO_SPLIT);
        if(canSplitCount == 0) {
            print("There are no chunks to split. Aborting ...");
        }
        else {
            // Step 4: Split the qualifying chunks
            if (DO_SPLIT == true) {
                print("Step 4: Splitting the qualifying chunks...");
                CHUNKS_TO_SPLIT.forEach(function(c) {
                    if(c.canSplit == true) {
                        var shardVersion = getShardVersion(c);
                        splitChunk(c, CON_MAP.get(c.shard), c.splitVector, shardVersion, CONFIGSVR);
                    };
                });

                // Step 5: Let's validate the splits outcome
                print("\nStep 5: Checking if the number of chunks has changed...");
                CHUNKS = [];
                getDatasizeArgs2(NS);
                getChunkCounts(CHUNKS);
            }
            else {
                print("Splits were not requested. Exiting...");
            };
        };
    };

    return;
});

/*

    var getDatasizeArgs = function(namespace, percentage=SAMPLE) {
        db.getSiblingDB("config").collections.find({_id: namespace}, {_id:1, key:1}).forEach(function(d) {
            var count = db.getSiblingDB("config").chunks.count({ ns: d._id });
            assert.lt(0, count, "The number of chunks to process is less than 1");
            var sampleValue = (db.getSiblingDB("config").chunks.count({ ns: d._id }) * percentage);
            if(sampleValue < 1)
                sampleValue = 1;
            db.getSiblingDB("config").chunks.aggregate([ {"$match": { ns: d._id }}, { $sample: { size: sampleValue } } ]).forEach(function(doc) {
                var newDoc = {
                    "datasize": doc.ns,
                    "keyPattern": d.key,
                    "min": doc.min,
                    "max": doc.max,
                    "estimate": OPTIMIZE_CHUNK_SIZE_CALCULATION,
                    "shard": doc.shard
                };
                CHUNKS.push(newDoc);
            });
        });
    };
    */