const distribution = require('../distribution');
const cors = require('cors');
const express = require('express');
const id = distribution.util.id;
const groupsTemplate = require('../distribution/all/groups');

const app = express();
const port = 3000;
app.use(cors());

process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;
global.nodeConfig = {ip: '127.0.0.1', port: 7070};

const crawlerGroup = {};
const nodes = [
  {ip: '127.0.0.1', port: 7110},
  {ip: '127.0.0.1', port: 7111},
  {ip: '127.0.0.1', port: 7112},
  {ip: '127.0.0.1', port: 7113},
  {ip: '127.0.0.1', port: 7114},
];

nodes.forEach((node) => {
  crawlerGroup[id.getSID(node)] = node;
});

const startNodes = (callback) => {
  let errors = [];
  let startedCount = 0;

  nodes.forEach((node) => {
    distribution.local.status.spawn(node, (e, v) => {
      if (e) {
        console.error(`Error starting node at ${node.ip}:${node.port}`, e);
        errors.push(e);
      } else {
        console.log(`Node started successfully at ${node.ip}:${node.port}`);
        startedCount++;
      }

      // Check if all nodes have finished processing
      if (startedCount + errors.length === nodes.length) {
        if (errors.length > 0) {
          callback(new Error('Failed to start all nodes'), null);
        } else {
          callback(null, 'All nodes started successfully');
        }
      }
    });
  });
};

const terminate = () => {
  console.log('-------------NODES CLEANING----------');
  let count = 0;
  nodes.forEach((node) => {
    let remote = {service: 'status', method: 'stop', node: node};
    distribution.local.comm.send([], remote, (e, v) => {
      if (++count === nodes.length) {
        console.log('All nodes stopped.');
      }
    });
  });
};

const cosSim = (score, candidates) => {
  let closest = candidates[0];
  let minDiff = Math.abs(score - closest.score);
  for (let i = 1; i < candidates.length; i++) {
    let currentDiff = Math.abs(score - candidates[i].score);
    if (currentDiff < minDiff) {
      closest = candidates[i];
      minDiff = currentDiff;
    }
  }
  return closest.url;
};


distribution.node.start((server) => {
  localServer = server;

  app.listen(port, () => {
    console.log(`Server listening on http://localhost:${port}`);
  });

  app.get('/search', (req, res) => {
    const searchTerm = req.query.term || 'default';
    const searchType = req.query.type || 'title';

    const crawlerConfig = {gid: 'crawler'};
    startNodes((e, v) => {
      if (e) {
        res.status(500).send('Failed to start all nodes, cannot proceed with search.');
        terminate();
        return;
      }

      groupsTemplate(crawlerConfig).put(crawlerConfig, crawlerGroup, (e, v) => {
        distribution.crawler.store.get(searchTerm, (e, v) => {
          console.log('Search results received:');
          let result = {};
          if (e) {
            result.message = 'No results found';
          } else if (searchType === 'title' && v.titleScores) {
            result.titleResult = cosSim(1, v.titleScores);
          } else if (searchType === 'author' && v.authorScores) {
            result.authorResult = cosSim(1, v.authorScores);
          }

          res.json(result);
          terminate();
        });
      });
    });
  });
});
