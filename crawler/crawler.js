global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;
const fs = require('fs');
const path = require('path');
const groupsTemplate = require('../distribution/all/groups');
const crawlerGroup = {};
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;

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
// const n4 = {ip: '127.0.0.1', port: 7113};
// const n5 = {ip: '127.0.0.1', port: 7114};
// const n6 = {ip: '127.0.0.1', port: 7115};
// const n7 = {ip: '127.0.0.1', port: 7116};
// const n8 = {ip: '127.0.0.1', port: 7117};
// const n9 = {ip: '127.0.0.1', port: 7118};

crawlerGroup[id.getSID(n1)] = n1;
crawlerGroup[id.getSID(n2)] = n2;
crawlerGroup[id.getSID(n3)] = n3;
// crawlerGroup[id.getSID(n4)] = n4;
// crawlerGroup[id.getSID(n5)] = n5;
// crawlerGroup[id.getSID(n6)] = n6;
// crawlerGroup[id.getSID(n7)] = n7;
// crawlerGroup[id.getSID(n8)] = n8;
// crawlerGroup[id.getSID(n9)] = n9;

const startNodes = (cb) => {
  distribution.local.status.spawn(n1, (e, v) => {
    distribution.local.status.spawn(n2, (e, v) => {
      distribution.local.status.spawn(n3, (e, v) => {
        cb();
        // distribution.local.status.spawn(n4, (e, v) => {
        //   distribution.local.status.spawn(n5, (e, v) => {
        //     distribution.local.status.spawn(n6, (e, v) => {
        //       distribution.local.status.spawn(n7, (e, v) => {
        //         distribution.local.status.spawn(n8, (e, v) => {
        //           distribution.local.status.spawn(n9, (e, v) => {
        //             cb();
        //           });
        //         });
        //       });
        //     });
        //   });
        // });
      });
    });
  });
};

// prepare dataset
let prepare = () => {
  const BASE_URL = 'https://atlas.cs.brown.edu/data/gutenberg/';
  let dataset = [];
  let index = 0;
  const book_path = path.join(__dirname, 'books_temp.txt'); // change to whole dataset when deploy
  const book_links = fs.readFileSync(book_path, 'utf-8').split('\n');
  book_links.forEach((line) => {
    let o = {};
    o[index] = path.join(BASE_URL, line.trim());
    dataset.push(o);
    index++;
  });
  return dataset;
}

//console.log(dataset.length, dataset[0]);

// map function
let m1 = async function(key, url) {
  let out = {};
  let book = {};
  book['url'] = url;
  const response = await global.fetch(url);
  const content = await response.text();
  const lines = content.split('\n');
  lines.forEach((line)=>{
    if (line.includes('Title')){
      const title = line.split(': ')[1];
      if (title === undefined) book['title'] = 'unknown';
      else book['title'] = title.trim();
    } else if (line.includes('Author')) {
      const author = line.split(': ')[1];
      if (author == undefined) book['author'] = 'anonymous';
      else book['author'] = author.trim();
    }
  });
  const tmp = url.split('/');
  const id_temp = tmp[tmp.length-1];
  const id = id_temp.split('.')[0];
  //console.log('------id: ',id_temp,id);
  out[id] = book;
  //console.log('book info: ', out);
  
  return out;
};

let r1 = function(key, value) {
  //console.log(`--------key: ${key}, value: ${value}-----`);
  let o = {};
  o[key] = value;
  return o;
};

const terminate = () => {
  console.log('-------------NODES CLEANING---------');
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        // remote.node = n4;
        // distribution.local.comm.send([], remote, (e, v) => {
        //   remote.node = n5;
        //   distribution.local.comm.send([], remote, (e, v) => {
        //     remote.node = n6;
        //     distribution.local.comm.send([], remote, (e, v) => {
        //       remote.node = n7;
        //       distribution.local.comm.send([], remote, (e, v) => {
        //         remote.node = n8;
        //         distribution.local.comm.send([], remote, (e, v) => {
        //           remote.node = n9;
        //           distribution.local.comm.send([], remote, (e, v) => {
        //             localServer.close();
        //           });
        //         });
        //       });
        //     });
        //  });
        // });
      });
    });
  });
};

const doMapReduce = () => {
  distribution.crawler.store.get(null, (e, v) => {
    console.log('Values and Error: ', e, v);
    distribution.crawler.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
      console.log('--------mr result: ', e, v);
      terminate();
    });
  });
};

distribution.node.start((server) => {
  localServer = server;
  const crawlerConfig = {gid: 'crawler'};
  startNodes(() => {
    groupsTemplate(crawlerConfig).put(crawlerConfig, crawlerGroup, (e, v) => {
      console.log('Put nodes into group: ', e, v);
      let cntr = 0;
      // We send the dataset to the cluster
      const dataset = prepare();
      //console.log('----------dataset:', dataset);
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
