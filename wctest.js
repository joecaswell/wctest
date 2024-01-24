const { MongoClient } = require("mongodb");
const { Worker, isMainThread, workerData, parentPort } = require('worker_threads');
const bson = require('bson');
const rg = require('./random_generation');
const stringify = bson.EJSON.stringify;

// getCircularReplacer taken from MDN
const getCircularReplacer = () => {
  const seen = new WeakSet();
  return (key, value) => {
    if (typeof value === "object" && value !== null) {
      if (seen.has(value)) {
        return;
      }
      seen.add(value);
    }
    return value;
  };
};

// Configuration section

// 0 = info only
// 1 = (currently unused)
// 2 = debug
const loglevel = 0;

// delay ietween tests in ms
const testdelay = 5000;

// cluster and credentials
const srvhost  = "cluster0.k8hsw.mongodb.net";
const username = "username";
const password = "password";

// URIs to be tested
const uriBase = `mongodb+srv://${username}:${password}@${srvhost}/test?readPreference=secondaryPreferred&readPreferenceTags=region:US_CENTRAL`;
const uris = [
 `${uriBase}&w=3`,
 `${uriBase}&w=twoRegions`,
 `${uriBase}&w=threeRegions`
];

// URI and per-host configs to connect to replica set members directly
const uri_left  = `mongodb://${username}:${password}@`;
const uri_right = "/test?tls=true&readPreference=secondary&authSource=admin&directConnection=true";
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

async function delay(promise, delayms) {
    return new Promise((resolve, reject) => {
        setTimeout( resolve(promise), delayms);
    })
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
    let oplogCursor = null;

    function info(msg) {
        parentPort.postMessage({tag:"info",region:region, msg:msg});
    }

    function debug(msg) {
        parentPort.postMessage({tag:"debug", region:region, msg:msg});
    }

    async function fetchTip() {
            let tipcursor = await local_client.db("local").collection("oplog.rs").find({}).sort({"$natural":-1}).limit(1);
            let tip = await tipcursor.next();
            debug("found oplog tip: " + stringify(tip));
            if (!tipcursor.closed) tipcursor.close();
            return(tip);
    }

    info("Worker starting")

    const local_client = new MongoClient(uri_left + host + uri_right);
    
    parentPort.on('message', async (message) => {
        debug("Received: " + stringify(message));
        message.msg = bson.EJSON.parse(message.msg);
    
        switch (message.tag) {
            case "stop": 
                running = false;
                debug("Worker stopping");

                if (oplogCursor) {
                    if (!oplogCursor.closed) {
                        await oplogCursor.close();
                    }
                }
                await local_client.close();
                debug("Worker shut down");
                process.exit(0);
                break;
            case "ack":
                let ackdoc = await fetchTip();
                info(`Oplog tip at ack of ${message.msg}: ${stringify(ackdoc.ts)} (${stringify(ackdoc.o2)})`);
                lastAck = message.msg;
                break;
            case "sent":
                expecting = message.msg;
                break;
            case "fetch_tip":
                if (loglevel >= 2) {
                    let tipdoc = await fetchTip();
                    debug(`Oplog tip: ${stringify(tipdoc.ts)} (${stringify(tipdoc.o2)})`);
                }
                break;
            default:
                info(`Unknown message type ${message.tag}`)
        }
    });

    async function stream() {
        if (!resumeToken) {
            let tip = await fetchTip();
            resumeToken = tip.ts;
        }

        let matchdoc = {
            ts: {"$gt": resumeToken},
            op: "i",
            ns: `${database}.${collection}`,
        };
        matchdoc[`o.${doctag}`] = {'$exists':true};

        let options = {tailable: true};

        oplogCursor = local_client.db("local").collection("oplog.rs").find(matchdoc, options);


        oplogCursor.stream().on('data', (doc) => {
            if (!oplogCursor.closed) {
                info(`Received ${doc.o._id} while expecting ${expecting} and last ack ${lastAck}`);
                resumeToken = doc.ts;
            }
        });

        oplogCursor.on('close', () => {
            debug("Oplog cursor stream closed");
            delete oplogCursor;
        });

    }

    async function checkStream() {
        if(running) {
            if (!oplogCursor) { 
                await stream();
                debug(`oplogCursor:${JSON.stringify(oplogCursor,getCircularReplacer())}`);
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
            let msgobj = Object.assign({},{tag, msg:stringify(msg)});
            debug("Broadcast: "+stringify(msgobj));
            for (w in workers) {
                workers[w].postMessage({tag:tag, msg:stringify(msg)});
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

            async function _sendTaggedInsert(doc, opts) { 
                if (!doc._id) {
                    doc = Object.assign({_id: new bson.ObjectId()}, doc);
                }
                debug(`Tagging ${stringify(doc)}`)
                doc[doctag] = true;

                // tell the workers which document is coming
                tellWorkers("sent", doc._id);
                info(`Insert ${doc._id} with options ${stringify(opts)}`);
                result = await coll.insertOne(doc, opts);
                debug("Result:" + stringify(result));

                let tipcursor = await client.db("local").collection("oplog.rs").find({}).sort({"$natural":-1}).limit(1);
                let tip = await tipcursor.next();
                if (!tipcursor.closed) tipcursor.close();
                info(`Primary oplog tip: ${stringify(tip.ts)} (${stringify(tip.o2)})`);

                tellWorkers("fetch_tip","");
                info(`Insert for ${doc._id} acknowledged`);
                tellWorkers("ack", doc._id);
            }

            async function sendTaggedInsert(doc, opts) {
                delay(_sendTaggedInsert(doc, opts), testdelay);
            }

            let status = await client.db("admin").command({replSetGetStatus:1});
            debug("replSetGetStatus: "+stringify(status));

            // visual separator between tests
            info("************************************************");

            let primary = status.members.filter(m=>m.state == 1).map(m=>m.name).join("");
            info("Current Primary: " +stringify(primary));

            info(`Begin test with uri: ${uri}`);

            await sendTaggedInsert({type:"first",dt:new Date()});
            
            await sendTaggedInsert({type:"second",dt:new Date()},{writeConcern:{w:"twoRegions"}});
            
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

            info("Send 1000 docs with w:1 to cause a bit of lag");
            await sendInserts(docs, {w:1});

            await sendTaggedInsert({type:"third",dt:new Date()},{});
            await sendTaggedInsert({type:"fourth",dt:new Date()},{writeConcern:{w:"twoRegions"}});
            await sendTaggedInsert({type:"fourth",dt:new Date()},{writeConcern:{w:"threeRegions"}});
        }
        
        async function testUris(remain) {
            if (!remain) {
                remain = uris
            }

            if (remain.length > 0){
                let uri = remain.shift();
                setTimeout(async function () {
                    await test(uri); 
                    testUris(remain);
                }, testdelay);
            } else {

                setTimeout( function() {
                    tellWorkers("stop", "Test Complete");
                    waitForWorkers()},
                    testdelay
                );
            }
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
                        info(stringify(message));
                }
            });
        }

    } else {
        monitor(workerData)
    }
}

main()
