const { MongoClient } = require("mongodb");
const { Worker, isMainThread, workerData, parentPort } = require('worker_threads');
const bson = require('bson');
const rg = require('./random_generation');

// Configuration section

// 0 = info only
// 2 = debug
const loglevel = 0;
// cluster and credentials
const srvhost  = "cluster0.k8hsw.mongodb.net";
const username = "username";
const password = "password";

// URIs to be tested
const uriBase = `mongodb+srv://${username}:${password}@${srvhost}/test?readPreference=secondaryPreferred&readPreferenceTags=region:US_CENTRAL`;
const uris = [
 `${uriBase}&w=3`,
 `${uriBase}&w=threeRegions`
];

// URI and per-host configs to connect to replica set members directly
const uri_left  = `mongodb://${username}:${password}@`;
const uri_right = "/test?tls=true&readPreference=secondary&authSource=admin";
const hosts = [
    {"region":"east1",    "host":"cluster0-shard-00-01.k8hsw.mongodb.net:27017"},
    {"region":"east2",    "host":"cluster0-shard-00-02.k8hsw.mongodb.net:27017"},
    {"region":"east3",    "host":"cluster0-shard-00-03.k8hsw.mongodb.net:27017"},
    {"region":"central", "host":"cluster0-shard-00-03.k8hsw.mongodb.net:27017"},
    {"region":"Ireland", "host":"cluster0-shard-00-04.k8hsw.mongodb.net:27017"}
];

// field to include in documents of interest
const doctag = "tagged_insert";

const database = "test";
const collection = "wctest";


function log(message) {
    console.log(new Date().toISOString(),message);
}

function info(message) {
    log("I " + message)
}

function debug(message) {
    if (loglevel >=2 ) {
        log("D " + message)
    }
}


// monitor function will be run in a thread for each replica set member monitored
async function monitor(Idx) {
    const regData = hosts[Idx]
    const region = regData.region;
    const host = regData.host;
    let running = true;
    let lastAck = null;
    let expecting = null;
    let resumeToken = null;
    let changestream = null;

    function info(msg) {
        parentPort.postMessage({tag:"info",region:region, msg:msg});
    }

    function debug(msg) {
        parentPort.postMessage({tag:"debug", region:region, msg:msg});
    }

    info("Worker starting")

    const local_client = new MongoClient(uri_left + host + uri_right);
    
    parentPort.on('message', async (message) => {
        debug("Received: " + bson.EJSON.stringify(message));
        message.msg = bson.EJSON.parse(message.msg);
    
        switch (message.tag) {
            case "stop": 
                running = false;
                debug("Worker stopping");

                if (changestream) {
                    if (!changestream.closed) {
                        await changestream.close();
                    }
                }
                await local_client.close();
                debug("Worker shut down");
                process.exit(0);
                break;
            case "ack":
                lastAck = message.msg;
                break;
            case "sent":
                expecting = message.msg;
                break;
            default:
                info(`Unknown message type ${message.tag}`)
        }
    });

    function stream() {
        let matchdoc = {"operationType":"insert"};
        matchdoc["fullDocument."+doctag] = true;
        const pipeline = [{"$match":matchdoc}];
        let options = {};
        if (resumeToken) {
            debug("Resuming changestream");
            options.resumeAfter = resumeToken;
        } else {
            debug("Starting change stream");
        }
        changestream = local_client.db(database).collection(collection).watch(pipeline, options);

        changestream.on('change', (doc) => {
            if (!changestream.closed) {
                info(`Received ${doc.documentKey._id} while expecting ${expecting} and last ack ${lastAck}`);
                resumeToken = doc._id;
            }
        });

        changestream.on('close', () => {
            debug("Change stream closed");
            delete changestream;
            changestream = "closed";
        });

    }

    function checkStream() {
        if(running) {
            if (!changestream) { 
                stream();
                debug(`changestream:${changestream}`);
                parentPort.postMessage({tag:"ready",region:region, msg:""});     
            }
            setTimeout(checkStream, 50);
        }
    }

    checkStream();

}

async function main() {
    if (isMainThread) {
        const workers = {};
        
        async function tellWorkers(tag, msg) {
            let msgobj = Object.assign({},{tag, msg:bson.EJSON.stringify(msg)});
            debug("Broadcast: "+bson.EJSON.stringify(msgobj));
            for (w in workers) {
                workers[w].postMessage({tag:tag, msg:bson.EJSON.stringify(msg)});
            }
        }

        function waitForWorkers() {
                debug(Object.keys(workers).length + " workers");
                if (Object.keys(workers).length == 0) {
                    info("All workers stopped");
                    process.exit(0);
                } else {
                    setTimeout(waitForWorkers,500);
                }
        }


        async function test(uri) {

            const client = new MongoClient(uri);
            const coll = client.db(database).collection(collection);

            async function sendInserts(docs, opts = {}) {
                return(coll.insertMany(docs, opts))
            }

            async function sendTaggedInsert(doc, opts) { 
                if (!doc._id) {
                    doc = Object.assign({_id: new bson.ObjectId()}, doc);
                }
                debug(`Tagging ${bson.EJSON.stringify(doc)}`)
                doc[doctag] = true;

                // tell the workers which document is coming
                tellWorkers("sent", doc._id);
                info(`Insert ${doc._id} with options ${bson.EJSON.stringify(opts)}`);
                result = await coll.insertOne(doc, opts);
                debug("Result:" + bson.EJSON.stringify(result));
                tellWorkers("ack", doc._id);
                info(`Insert for ${doc._id} acknowledged`);
            }

            info(`Begin test with uri: ${uri}`);

            await sendTaggedInsert({type:"first",dt:new Date()});
            
            await sendTaggedInsert({type:"second",dt:new Date()},{writeConcern:{w:"threeRegions"}});

            let docs = []
            for(i=0;i<1000;i++){
                docs.push({
                        type:"random",
                        name:rg.randomString(),
                        dt: new Date(),
                        int: rg.randomInt()
                })
            };

            await sendInserts(docs);

            await sendTaggedInsert({type:"third",dt:new Date()},{});
            await sendTaggedInsert({type:"fourth",dt:new Date()},{writeConcern:{w:"threeRegions"}});
        }
        
        async function testUris() {

            for(let i=0;i<uris.length;i++){
                await test(uris[i]);
                debug(`Complete ${uris[i]}`)
            };    

            setTimeout( function() {
                tellWorkers("stop", "Test Complete");
                waitForWorkers()},
                5000
            );
        } 

        let started = {};

        for (regIdx in hosts) {
            let region = hosts[regIdx].region;
            info(`Spawn ${region} monitor`);
            let worker = new Worker(__filename, {workerData: regIdx});
            workers[region] = worker;
            worker.on('exit', () => {
                info(`Worker ${region} exited`);
                delete workers[region];
            });
            worker.on('message', (message) => {
                switch(message.tag) {
                    case "debug":
                        debug(`${message.region}: ${message.msg}`); 
                        break;
                    case "info":
                        info(`${message.region}: ${message.msg}`); 
                        break;
                    case "ready":
                        info(`${message.region} ready`);
                        started[message.region] = true;
                        if (Object.keys(started).length == hosts.length) testUris();
                        break;
                    default:
                        info(bson.EJSON.stringify(message));
                }
            });
        }

    } else {
        monitor(workerData)
    }
}

main()
