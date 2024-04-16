const id = require('../util/id');

global.kvHashMap = {};

const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';

  return {
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */
      const {keys, map, reduce, compact} = configuration;

      // Setup phase
      const mrService = {};

      mrService.mapper = (keys, map, compact, gid, callback) => {
        let allRes = [];
        const storeFindConfig = {key: null, gid: gid};
        global.distribution.local.store.get(storeFindConfig, (e, v) => {
          let matchedKeys = [];
          for (let i = 0; i < v.length; i++) {
            let key = v[i];
            if (keys.includes(key)) {
              matchedKeys.push(key);
            }
          }
          let count = 0;
          if (count === matchedKeys.length) {
            callback(e, allRes);
          }
          for (let key of matchedKeys) {
            const singleConfig = {key: key, gid: gid};
            global.distribution.local.store.get(singleConfig, (e, value) => {
              if (e != null) {
                callback(new Error('Local Store Get Error'), null);
              }

              let res = map(key, value);
              if (compact != null) {
                console.log('hello');
                res = compact(res);
              } else {
                console.log('Compact is null');
              }
              console.log('Map res: ', key, value, Array.isArray(res), res);

              global.distribution.local.store.put(res,
                  singleConfig, (e, v) => {
                    console.log('hello');
                    count++;
                    if (count === matchedKeys.length) {
                      callback(e, allRes);
                    }
                  });
            });
          }
        });
      };

      mrService.reducer = (keys, reduce, gid, callback) => {
        const storeFindConfig = {key: null, gid: gid};
        global.distribution.local.store.get(storeFindConfig, (e, v) => {
          let matchedKeys = [];
          for (let i = 0; i < v.length; i++) {
            let key = v[i];
            if (keys.includes(key)) {
              matchedKeys.push(key);
            }
          }
          let count = 0;
          // console.log('REDUCE MATCHED KEYS: ', v, matchedKeys);
          if (count === matchedKeys.length) {
            callback(e, []);
          }
          let allRes = [];
          for (let key of matchedKeys) {
            const singleConfig = {key: key, gid: gid};
            global.distribution.local.store.get(singleConfig, (e, value) => {
              if (e != null) {
                callback(new Error('Local Store Get Error'), null);
              }

              let res = reduce(key, value);
              allRes.push(res);
              console.log('Get result: ', res);
              count++;
              if (count === matchedKeys.length) {
                callback(e, allRes);
              }
            });
          }
        });
      };

      mrService.shuffle = (keys, gid, callback) => {
        const storeFindConfig = {key: null, gid: gid};
        global.distribution.local.store.get(storeFindConfig, (e, v) => {
          let matchedKeys = [];
          for (let i = 0; i < v.length; i++) {
            let key = v[i];
            if (keys.includes(key)) {
              matchedKeys.push(key);
            }
          }
          let count = 0;
          let expectedCount = 0;
          let allKeys = [];
          if (matchedKeys.length === 0) {
            callback(e, allKeys);
          }
          for (let key of matchedKeys) {
            const singleConfig = {key: key, gid: gid};
            global.distribution.local.store.get(singleConfig, (e, value) => {
              if (e != null) {
                callback(new Error('Local Store Get Error'), null);
              }
              if (Array.isArray(value)) {
                expectedCount += value.length;
                for (let obj of value) {
                  let keyList = Object.keys(obj);
                  const k = keyList[0];
                  allKeys.push(k);
                  const valueShuffle = obj[keyList[0]];
                  global.distribution[gid].store.append(valueShuffle,
                      k, (e, v) => {
                        count++;
                        // console.log('Content list after append: ', v);
                        if (count === expectedCount) {
                          callback(e, allKeys);
                        }
                      });
                }
              } else {
                let keyList = Object.keys(value);
                const k = keyList[0];
                allKeys.push(k);
                const valueShuffle = value[keyList[0]];
                global.distribution[gid].store.append(valueShuffle,
                    k, (e, v) => {
                      count++;
                      // console.log('Content list after append: ', v);
                      if (count === matchedKeys.length) {
                        callback(e, allKeys);
                      }
                    });
              }
            });
          }
        });
      };

      const mrServiceName = 'mr_' + id.getID(mrService);

      global.distribution[context.gid].routes.put(mrService,
          mrServiceName, (e, v) => {
            console.log('Finish Setup!');
            let remote = {
              service: mrServiceName,
              method: 'mapper',
            };
            let message = [keys, map, compact, context.gid];
            global.distribution[context.gid].comm.send(message,
                remote, (e, v) => {
                  console.log('Finish Map!');
                  // console.log('Get map value: ', v);

                  let message = [keys, context.gid];
                  let remote = {
                    service: mrServiceName,
                    method: 'shuffle',
                  };
                  global.distribution[context.gid].comm.send(message,
                      remote, (e, keyMap) => {
                        console.log('Get shuffle value: ', keyMap);
                        let keySet = new Set();
                        for (let keys of Object.values(keyMap)) {
                          for (let key of keys) {
                            keySet.add(key);
                          }
                        }
                        console.log('KeySet: ', keySet);
                        let message = [[...keySet], reduce, context.gid];
                        let remote = {
                          service: mrServiceName,
                          method: 'reducer',
                        };
                        global.distribution[context.gid].comm.send(message,
                            remote, (e, res) => {
                              let finalRes = [];
                              for (let resList of Object.values(res)) {
                                for (let r of resList) {
                                  finalRes.push(r);
                                }
                              }
                              console.log('Final res: ', finalRes);
                              callback(null, finalRes);
                            });
                      });
                });
          });
    },
  };
};

module.exports = mr;
