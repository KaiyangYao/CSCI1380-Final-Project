global.fetch = require("node-fetch");
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

global.nodeConfig = { ip: "127.0.0.1", port: 7070 };
const distribution = require("../distribution");
const id = distribution.util.id;

const groupsTemplate = require("../distribution/all/groups");

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

const n1 = { ip: "127.0.0.1", port: 7110 };
const n2 = { ip: "127.0.0.1", port: 7111 };
const n3 = { ip: "127.0.0.1", port: 7112 };
const n4 = { ip: "127.0.0.1", port: 7113 };
const n5 = { ip: "127.0.0.1", port: 7114 };

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
  console.log("-------------NODES CLEANING----------");
  let remote = { service: "status", method: "stop" };
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
  const contentTitle = obj[0].title;
  const contentAuthor = obj[0].author;
  const url = obj[0].url;

  const createNGrams = (words, n) => {
    const ngrams = [];
    for (let i = 0; i < words.length - n + 1; i++) {
      ngrams.push(words.slice(i, i + n).join(" "));
    }
    return ngrams;
  };

  const calculateTermFrequencies = (content) => {
    const termFrequency = {};
    const words = tokenizer
      .tokenize(content.toLowerCase())
      .map((word) => stemmer.stem(word));
    const oneGrams = words;
    const biGrams = createNGrams(words, 2);
    const allTerms = [...oneGrams, ...biGrams];
    const totalTerms = allTerms.length;
    allTerms.forEach((term) => {
      termFrequency[term] = (termFrequency[term] || 0) + 1;
    });
    return { termFrequency, totalTerms };
  };

  const titleData = calculateTermFrequencies(contentTitle);
  const authorData = calculateTermFrequencies(contentAuthor);

  const output = [];

  const processTerms = (data, tfLabel) => {
    for (const [term, count] of Object.entries(data.termFrequency)) {
      const normalizedFrequency = count / data.totalWords;
      let entry = output.find((o) => Object.keys(o)[0] === term);
      if (!entry) {
        entry = { [term]: { url: url } };
        output.push(entry);
      }
      entry[term][tfLabel] = normalizedFrequency;
    }
  };

  processTerms(titleData, "titleTF");
  processTerms(authorData, "authorTF");

  return output;
};

let indexReduce = (term, values) => {
  const N = 1000;
  let out = {};

  const calculateIDF = (documentCount) =>
    documentCount > 0 ? 1 + Math.log(N / documentCount) : 0;

  const calculateScores = (entries, idf) => {
    return entries.map((entry) => ({ url: entry.url, score: entry.tf * idf }));
  };

  const titleEntries = values.filter((v) => v.titleTF !== undefined);
  const authorEntries = values.filter((v) => v.authorTF !== undefined);

  const titleIDF = calculateIDF(titleEntries.length);
  const authorIDF = calculateIDF(authorEntries.length);

  // eslint-disable-next-line max-len
  const titleScores =
    titleEntries.length > 0
      ? calculateScores(
          titleEntries.map((entry) => ({ url: entry.url, tf: entry.titleTF })),
          titleIDF
        )
      : [];
  // eslint-disable-next-line max-len
  const authorScores =
    authorEntries.length > 0
      ? calculateScores(
          authorEntries.map((entry) => ({
            url: entry.url,
            tf: entry.authorTF,
          })),
          authorIDF
        )
      : [];

  if (titleScores.length > 0) {
    out.titleTF = titleEntries;
    out.titleIDF = titleIDF;
    out.titleScores = titleScores;
  }
  if (authorScores.length > 0) {
    out.authorTF = authorEntries;
    out.authorIDF = authorIDF;
    out.authorScores = authorScores;
  }

  console.log("term: ", term);
  if (titleScores.length > 0) {
    console.log("Title TF: ", titleEntries);
    console.log("Title IDF: ", titleIDF);
    console.log("Title Scores: ", titleScores);
  }
  if (authorScores.length > 0) {
    console.log("Author TF: ", authorEntries);
    console.log("Author IDF: ", authorIDF);
    console.log("Author Scores: ", authorScores);
  }

  return out;
};

const doIndexMapReduce = (cb) => {
  distribution.crawler.store.get(null, (e, v) => {
    distribution.crawler.mr.exec(
      { keys: v, map: indexMap, reduce: indexReduce, storeReducedValue: true },
      (e, v) => {
        terminate();
      }
    );
  });
};

distribution.node.start((server) => {
  localServer = server;
  const crawlerConfig = { gid: "crawler" };
  startNodes(() => {
    groupsTemplate(crawlerConfig).put(crawlerConfig, crawlerGroup, (e, v) => {
      doIndexMapReduce();
    });
  });
});
