global.fetch = require('node-fetch');

global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

const ncdc1Group = {};
const dlib1Group = {};
const crawlerGroup = {};
const regexGroup = {};
const rwlgGroup = {};
const student1Group = {};
const student2Group = {};

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

beforeAll((done) => {
  /* Stop the nodes if they are running */

  ncdc1Group[id.getSID(n1)] = n1;
  ncdc1Group[id.getSID(n2)] = n2;
  ncdc1Group[id.getSID(n3)] = n3;

  dlib1Group[id.getSID(n1)] = n1;
  dlib1Group[id.getSID(n2)] = n2;
  dlib1Group[id.getSID(n3)] = n3;

  crawlerGroup[id.getSID(n1)] = n1;
  crawlerGroup[id.getSID(n2)] = n2;
  crawlerGroup[id.getSID(n3)] = n3;

  student1Group[id.getSID(n1)] = n1;
  student1Group[id.getSID(n2)] = n2;
  student1Group[id.getSID(n3)] = n3;

  student2Group[id.getSID(n1)] = n1;
  student2Group[id.getSID(n2)] = n2;
  student2Group[id.getSID(n3)] = n3;

  regexGroup[id.getSID(n1)] = n1;
  regexGroup[id.getSID(n2)] = n2;
  regexGroup[id.getSID(n3)] = n3;

  rwlgGroup[id.getSID(n1)] = n1;
  rwlgGroup[id.getSID(n2)] = n2;
  rwlgGroup[id.getSID(n3)] = n3;


  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const ncdc1Config = {gid: 'ncdc1'};
    startNodes(() => {
      groupsTemplate(ncdc1Config).put(ncdc1Config, ncdc1Group, (e, v) => {
        const dlib1Config = {gid: 'dlib1'};
        groupsTemplate(dlib1Config).put(dlib1Config, dlib1Group, (e, v) => {
          const crawlerConfig = {gid: 'crawler'};
          groupsTemplate(crawlerConfig).put(crawlerConfig,
              crawlerGroup, (e, v) => {
                const student1Config = {gid: 'student1'};
                groupsTemplate(student1Config).put(student1Config,
                    student1Group, (e, v) => {
                      const student2Config = {gid: 'student2'};
                      groupsTemplate(student2Config).put(student2Config,
                          student2Group, (e, v) => {
                            const regexConfig = {gid: 'regex'};
                            groupsTemplate(regexConfig).put(regexConfig,
                                regexGroup, (e, v) => {
                                  const rwlgConfig = {gid: 'rwlg'};
                                  groupsTemplate(rwlgConfig).put(rwlgConfig,
                                      rwlgGroup, (e, v) => {
                                        done();
                                      });
                                });
                          });
                    });
              });
        });
      });
    });
  });
});

afterAll((done) => {
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});

function sanityCheck(mapper, reducer, dataset, expected, done) {
  let mapped = dataset.map((o) =>
    mapper(Object.keys(o)[0], o[Object.keys(o)[0]]));
  /* Flatten the array. */
  mapped = mapped.flat();
  let shuffled = mapped.reduce((a, b) => {
    let key = Object.keys(b)[0];
    if (a[key] === undefined) a[key] = [];
    a[key].push(b[key]);
    return a;
  }, {});
  let reduced = Object.keys(shuffled).map((k) => reducer(k, shuffled[k]));

  try {
    expect(reduced).toEqual(expect.arrayContaining(expected));
  } catch (e) {
    done(e);
  }
}

// ---all.mr---
test('(25 pts) Compact test 1', (done) => {
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

  let c1 = (res) => {
    let out = {};
    if (Array.isArray(res)) {
      console.log('Is array', res);
      for (let obj of res) {
        let keyList = Object.keys(obj);
        const k = keyList[0];
        const value = obj[k];
        if (k in out) {
          out[k] += value;
        } else {
          out[k] = value;
        }
      }
      // out = Object.entries(out).map(([key, value]) => ({[key]: value}));
      const finalRes = [];
      Object.keys(out).forEach((key) => {
        finalRes.push({[key]: out[key]});
      });
      return finalRes;
    } else {
      return res;
    }
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{'1950': 22}, {'1949': 111}];

  /* Sanity check: map and reduce locally */
  sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc1.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }


      distribution.ncdc1.mr.exec({keys: v, map: m1, reduce: r1,
        compact: c1}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc1.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(25 pts) Compact test 2', (done) => {
  let m2 = (key, value) => {
    // map each word to a key-value pair like {word: 1}
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    let out = [];
    words.forEach((w) => {
      let o = {};
      o[w] = 1;
      out.push(o);
    });
    return out;
  };

  let r2 = (key, values) => {
    let out = {};
    sum = 0;
    values.forEach((num) => {
      sum += num;
    });
    out[key] = sum;
    return out;
  };

  let c2 = (res) => {
    let out = {};
    if (Array.isArray(res)) {
      console.log('Is array', res);
      for (let obj of res) {
        let keyList = Object.keys(obj);
        const k = keyList[0];
        const value = obj[k];
        if (k in out) {
          out[k] += value;
        } else {
          out[k] = value;
        }
      }
      // out = Object.entries(out).map(([key, value]) => ({[key]: value}));
      const finalRes = [];
      Object.keys(out).forEach((key) => {
        finalRes.push({[key]: out[key]});
      });
      return finalRes;
    } else {
      return res;
    }
  };

  let dataset = [
    {'b1-l1': 'It was the best of times, it was the worst of times,'},
    {'b1-l2': 'it was the age of wisdom, it was the age of foolishness,'},
    {'b1-l3': 'it was the epoch of belief, it was the epoch of incredulity,'},
    {'b1-l4': 'it was the season of Light, it was the season of Darkness,'},
    {'b1-l5': 'it was the spring of hope, it was the winter of despair,'},
  ];

  let expected = [
    {It: 1}, {was: 10},
    {the: 10}, {best: 1},
    {of: 10}, {'times,': 2},
    {it: 9}, {worst: 1},
    {age: 2}, {'wisdom,': 1},
    {'foolishness,': 1}, {epoch: 2},
    {'belief,': 1}, {'incredulity,': 1},
    {season: 2}, {'Light,': 1},
    {'Darkness,': 1}, {spring: 1},
    {'hope,': 1}, {winter: 1},
    {'despair,': 1},
  ];

  /* Sanity check: map and reduce locally */
  sanityCheck(m2, r2, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.dlib1.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.dlib1.mr.exec({keys: v, map: m2, reduce: r2,
        compact: c2}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.dlib1.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(25 pts) student test 4', (done) => {
  let m4 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    console.log(words);
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let r4 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.min(a, b), Infinity);
    return out;
  };


  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{'1950': -11}];

  /* Sanity check: map and reduce locally */
  // sanityCheck(m4, r4, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.student1.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.student1.mr.exec({keys: v, map: m4,
        reduce: r4}, (e, v) => {
        try {
          console.log(expected.length);
          console.log(v.length);
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.student1.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(25 pts) student test 5', (done) => {
  let m5 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    console.log(words);
    let out = {};
    out[parseInt(words[3])] = parseInt(words[1]);
    return out;
  };

  let r5 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
    return out;
  };


  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
    {'524': '004301265099999 1979 0324180040500001N9 +0111 1+9999'},
  ];

  let expected = [{111: 1979}, {0: 1950}, {78: 1949}, {22: 1950}];

  /* Sanity check: map and reduce locally */
  // sanityCheck(m4, r4, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.student2.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.student2.mr.exec({keys: v, map: m5,
        reduce: r5}, (e, v) => {
        try {
          console.log(expected.length);
          console.log(v.length);
          expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.student2.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(25 pts) crawler test', (done) => {
  let m1 = async function(key, url) {
    let o = {};
    try {
      const response = await global.fetch(url);
      const html = await response.text();
      o['url1'] = html;
      console.log('HTML: ', html);
    } catch (error) {
      console.error('Error extracting text from URL:', error);
    }
    return o;
  };

  let r1 = (key, value) => {
    let out = {};
    out[key] = value;
    return out;
  };

  let dataset = [{0: 'https://atlas.cs.brown.edu/data/gutenberg/'}];

  // const contentPath = path.join(__dirname, '../search/example-page.txt');
  // const content = fs.readFileSync(contentPath, 'utf8');
  // console.log(content);
  // let expected = [
  //   {
  //     'http://example.com': [],
  //   },
  // ];

  /* Sanity check: map and reduce locally */
  // sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.crawler.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.crawler.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          // expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.crawler.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

// test('(25 pts) Regex Matching test', (done) => {
//   let m1 = (key, value) => {
//     const regexPattern = /^\d{3}$/;
//     let out = {};
//     if (regexPattern.test(key)) {
//       out['true'] = key;
//     }
//     return out;
//   };

//   let r1 = (key, values) => {
//     if (key == 'true') {
//       return values;
//     }
//   };

//   let dataset = [
//     {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
//     {'224': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
//     {'1114': '004301265099999 1911 0324180040500001N9 +0078 1+9999'},
//   ];

//   let expected = [['000', '224']];

//   /* Sanity check: map and reduce locally */
//   // sanityCheck(m1, r1, dataset, expected, done);

//   /* Now we do the same thing but on the cluster */
//   const doMapReduce = (cb) => {
//     distribution.regex.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }


//       distribution.regex.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
//         try {
//           expect(v).toEqual(expected);
//           done();
//         } catch (e) {
//           done(e);
//         }
//       });
//     });
//   };

//   let cntr = 0;

//   // We send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.regex.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
// });

// test('(25 pts) Reverse web link graph test', (done) => {
//   let m1 = (key, value) => {
//     let keys = Object.keys(value);
//     console.log('value: ', value);
//     console.log('type: ', typeof(value));
//     out = [];
//     for (let i = 0; i < keys.length; i++) {
//       let k = keys[i];
//       let v = value[k];
//       let o = {};
//       o[v] = k;
//       out.push(o);
//     }
//     return out;
//   };

//   let r1 = (key, values) => {
//     let out = {};
//     out[key] = values;
//     return out;
//   };

//   let dataset = [
//     {'000': {'source 1': 'sink link 1', 'source 2': 'sink link 1',
//       'source 3': 'sink link 2'}},
//     {'111': {'source 4': 'sink link 2', 'source 5': 'sink link 3',
//       'source 6': 'sink link 1'}},
//   ];

//   let expected = [{'sink link 1': ['source 1', 'source 2', 'source 6']},
//     {'sink link 2': ['source 3', 'source 4']}, {'sink link 3': ['source 5']}];

//   /* Sanity check: map and reduce locally */
//   // sanityCheck(m1, r1, dataset, expected, done);

//   /* Now we do the same thing but on the cluster */
//   const doMapReduce = (cb) => {
//     distribution.rwlg.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }


//       distribution.rwlg.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
//         try {
//           expect(v).toEqual(expect.arrayContaining(expected));
//           done();
//         } catch (e) {
//           done(e);
//         }
//       });
//     });
//   };

//   let cntr = 0;

//   // We send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.rwlg.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
// });
