(function(NS) {

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

    // Configurable globals go here
    var SAMPLE = 1;
    var OPTIMIZE_CHUNK_SIZE_CALCULATION = true; // use avg doc size to calculate chunk sizes
    var DOUBLECHECK_CHUNK_SIZE = false; // in optimized mode, if split threshold is exceeded check the actual chunk size
    var CONFIGSVR = db.serverStatus().sharding.configsvrConnectionString;
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
        return shardConnection.getDB("admin").runCommand({
            "splitChunk": chunk.datasize,
            "from": chunk.shard,
            "min": chunk.min,
            "max": chunk.max,
            "keyPattern": chunk.keyPattern,
            "splitKeys": splitVector,
            "configdb": configDB,
            "shardVersion": shardVersion
        });
    };

    // A couple of small helpers
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
        print("Identified " + chunkArray.length + " chunks for " + chunkArray[0].datasize + " collection");
    };

    var getSplitChunkCounts = function(chunkArray) {
        var counter = 0;
        chunkArray.forEach(function(c) {
            if(c.canSplit == true) {
                counter++;
            };
        });
        print("Identified " + counter + " chunks that can be split");
    };

    /// MAIN SECTION

    // Step 1: Get the sample chunks
    getDatasizeArgs(NS);
    getChunkCounts(CHUNKS);
    // Step 2: Filter out the qualifying chunks
    CHUNKS.forEach(function(c){ 
        var chunkSize = checkChunkSize2(c);
        if(chunkSize >= 0) {
            var chunkToSplit = c;
            chunkToSplit.chunkSize = chunkSize;
            chunkToSplit.canSplit = false;
            CHUNKS_TO_SPLIT.push(chunkToSplit);      
        };
    });

    // Step 3: Obtain split points

    CHUNKS_TO_SPLIT.forEach(function(c) {
        getSplitVector(c);
    });
    getSplitChunkCounts(CHUNKS_TO_SPLIT);
    // Step 3: Split the qualifying chunks

    CHUNKS_TO_SPLIT.forEach(function(c) {
        if(c.canSplit == true) {
            var shardVersion = getShardVersion(c);
            splitChunk(c, CON_MAP.get(c.shard), c.splitVector, shardVersion, CONFIGSVR);
        };
    });
});