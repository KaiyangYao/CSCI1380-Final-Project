const distribution = require('../distribution');
const cors = require('cors');
const express = require('express');
const path = require('path');
const id = distribution.util.id;
const groupsTemplate = require('../distribution/all/groups');
const checkSpelling = require('./spellcheck');

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

startNodes((error, message) => {
  if (error) {
    console.error('Error starting nodes:', error);
    return;
  }
  console.log(message);
});

// Stop nodes only when the process is terminating
process.on('SIGINT', () => {
  terminate();
  process.exit();
});

const findClosestScores = (targetScore, candidates) => {
  candidates.sort((a, b) => {
    const diffA = Math.abs(targetScore - a.score);
    const diffB = Math.abs(targetScore - b.score);
    return diffA - diffB;
  });

  return candidates.slice(0, 10).map((candidate) => ({
    url: candidate.url,
    title: candidate.title,
    author: candidate.author,
    language: candidate.language,
  }));
};

const createNGrams = (words, n) => {
  if (n < 1 || words.length < n) return [];
  let nGrams = [];
  for (let i = 0; i <= words.length - n; i++) {
    nGrams.push(words.slice(i, i + n).join(' '));
  }
  return nGrams;
};

app.use(express.static(path.join(__dirname, '../frontend')));

app.listen(port, () => {
  console.log(`Server listening on http://localhost:${port}`);
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/index.html'));
});

app.get('/search', (req, res) => {
  const searchTerm = req.query.term || 'default';
  const searchType = req.query.type || 'title';
  const tokenizer = new global.natural.WordTokenizer();
  const words = tokenizer.tokenize(searchTerm.toLowerCase());
  const oneGrams = words;
  const biGrams = createNGrams(words, 2);
  // We will list the bi-grams closest result first
  const allTerms = [...biGrams, ...oneGrams];

  let aggregatedResults = {
    titleResponse: [],
    authorResponse: [],
    suggestions: [],
  };

  let visited = new Set();
  let totalTerms = allTerms.length;
  let count = 0;

  const checkCompletion = () => {
    if (count === totalTerms) {
      let response = {noMatch: false};

      if (searchType === 'title') {
        if (aggregatedResults.titleResponse.length === 0) {
          response.noMatch = true;
          response.results = aggregatedResults.suggestions;
        } else {
          response.results = aggregatedResults.titleResponse.flat();
        }
      } else if (searchType === 'author') {
        if (aggregatedResults.authorResponse.length === 0) {
          response.noMatch = true;
          response.results = aggregatedResults.suggestions;
        } else {
          response.results = aggregatedResults.authorResponse.flat();
        }
      }

      if (!res.headersSent) {
        res.json(response);
      }
    }
  };

  const crawlerConfig = {gid: 'crawler'};
  groupsTemplate(crawlerConfig).put(crawlerConfig, crawlerGroup, (e, v) => {
    allTerms.forEach((term) => {
      distribution.crawler.store.get(term, (e, v) => {
        if (e) {
          aggregatedResults.suggestions = checkSpelling(term);
        } else {
          let scores = (searchType === 'title' ? v.titleScores : v.authorScores);
          if (scores) {
            findClosestScores(1, scores).forEach((candidate) => {
              if (!visited.has(candidate.title)) {
                visited.add(candidate.title);
                if (searchType === 'title') {
                  aggregatedResults.titleResponse.push(candidate);
                } else {
                  aggregatedResults.authorResponse.push(candidate);
                }
              }
            });
          }
        }
        count++;
        checkCompletion();
      });
    });
  });
});
