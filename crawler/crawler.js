global.fetch = require('node-fetch');
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


crawlerGroup[id.getSID(n1)] = n1;
crawlerGroup[id.getSID(n2)] = n2;
crawlerGroup[id.getSID(n3)] = n3;


const startNodes = (cb) => {
  distribution.local.status.spawn(n1, (e, v) => {
    distribution.local.status.spawn(n2, (e, v) => {
      distribution.local.status.spawn(n3, (e, v) => {
        cb();
      });
    });
  });
};


let m1 = (key, value) => {
  let words = value.split(/(\s+)/).filter((e) => e !== ' ');
  console.log(words);
  let out = {};
  out[words[1]] = parseInt(words[3]);
  return out;
};

let r1 = (key, values) => {
  let out = {};
  out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
  return out;
};

let dataset = [
  {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
  {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
  {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
  {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
  {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
];

const terminate = () => {
  console.log('------------------------------NODES CLEANING...');
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
      });
    });
  });
};

const doMapReduce = () => {
  distribution.crawler.store.get(null, (e, v) => {
    console.log('Keys: ', v);
    console.log('Values and Error: ', e, v);
    distribution.crawler.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
      try {
        // expect(v).toEqual(expect.arrayContaining(expected));
        // done();
        console.log('Value: ', v);
      } catch (e) {
        // done(e);
        console.log('Error Crawl');
      }
      terminate();
    });
  });
};

// let crawl = async (dataset) => {
//   let cntr = 0;
//   // We send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.crawler.store.put(value, key, async (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         await doMapReduce();
//       }
//     });
//   });
// };


distribution.node.start((server) => {
  localServer = server;
  const crawlerConfig = {gid: 'crawler'};
  startNodes(() => {
    groupsTemplate(crawlerConfig).put(crawlerConfig,
        crawlerGroup, (e, v) => {
          console.log('Put nodes into group: ', e, v);
          let cntr = 0;
          // We send the dataset to the cluster
          dataset.forEach((o) => {
            let key = Object.keys(o)[0];
            let value = o[key];
            distribution.crawler.store.put(value, key, (e, v) => {
              if (!e) {
                cntr++;
                // Once we are done, run the map reduce
                if (cntr === dataset.length) {
                  doMapReduce();
                }
              }
            });
          });
        });
  });
});

