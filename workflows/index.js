global.fetch = require('node-fetch');
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;

global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

const crawlerGroup = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};
const n4 = {ip: '127.0.0.1', port: 7113};
const n5 = {ip: '127.0.0.1', port: 7114};


crawlerGroup[id.getSID(n1)] = n1;
crawlerGroup[id.getSID(n2)] = n2;
crawlerGroup[id.getSID(n3)] = n3;
crawlerGroup[id.getSID(n4)] = n4;
crawlerGroup[id.getSID(n5)] = n5;


const startNodes = (cb) => {
  distribution.local.status.spawn(n1, (e, v) => {
    distribution.local.status.spawn(n2, (e, v) => {
      distribution.local.status.spawn(n3, (e, v) => {
        distribution.local.status.spawn(n4, (e, v) => {
          distribution.local.status.spawn(n5, (e, v) => {
            cb();
          });
        });
      });
    });
  });
};

const terminate = () => {
  console.log('-------------NODES CLEANING----------');
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            localServer.close();
          });
        });
      });
    });
  });
};


let indexMap = (fileName, obj) => {
  const content = obj[0].title;
  const url = obj[0].url;
  const termFrequency = {};
  const words = content.toLowerCase().match(/\w+/g) || [];
  const totalWords = words.length;
  const output = [];

  words.forEach((word) => {
    termFrequency[word] = (termFrequency[word] || 0) + 1;
  });

  for (const [term, count] of Object.entries(termFrequency)) {
    // Normalize term frequency by the total number of words in the document
    const normalizedFrequency = count / totalWords;
    let o = {};
    o[term] = {url: url, tf: normalizedFrequency};
    output.push(o);
  }

  return output;
};


let indexReduce = (term, values) => {
  const N = 3;
  let out = {};
  let idf = 1 + Math.log(N / values.length);
  let scores = values.map((entry) => ({url: entry.url, score: entry.tf * idf}));

  console.log('term: ', term);
  console.log('tf: ', values);
  console.log('idf: ', idf);
  console.log('scores: ', scores);
  out = {
    tf: values,
    idf: idf,
    score: scores,
  };
  return out;
};

const doIndexMapReduce = (cb) => {
  distribution.crawler.store.get(null, (e, v) => {
    distribution.crawler.mr.exec({keys: v, map: indexMap, reduce: indexReduce, storeReducedValue: true}, (e, v) => {
      terminate();
    });
  });
};


distribution.node.start((server) => {
  localServer = server;
  const crawlerConfig = {gid: 'crawler'};
  startNodes(() => {
    groupsTemplate(crawlerConfig).put(crawlerConfig,
        crawlerGroup, (e, v) => {
          doIndexMapReduce();
        });
  });
});
