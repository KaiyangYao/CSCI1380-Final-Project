const id = require('../util/id');


const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';

  return {
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */
<<<<<<< Updated upstream
      const {keys, map, reduce, compact} = configuration;
=======
      const {keys, map, reduce, compact, storeReducedValue, batch} = configuration;
>>>>>>> Stashed changes

      // Setup phase
      const mrService = {};

      mrService.mapper = (keys, map, compact, gid, batch, callback) => {
        console.log('mapper get called');
        let allRes = [];
        const storeFindConfig = {key: null, gid: gid, batch: batch};
        global.distribution.local.store.get(storeFindConfig, (e, v) => {
          let matchedKeys = [];
          for (let i = 0; i < v.length; i++) {
            let key = v[i];
            if (keys.includes(key)) {
              matchedKeys.push(key);
            }
          }
          let count = 0;
          console.log('Mapper matchedKeys: ', matchedKeys);
          if (count === matchedKeys.length) {
            callback(e, allRes);
          }
          for (let key of matchedKeys) {
            const singleConfig = {key: key, gid: gid, batch: batch};
            global.distribution.local.store.get(singleConfig,
                async (e, value) => {
                  if (e != null) {
                    callback(new Error('Local Store Get Error'), null);
                  }
                  let res = await map(key, value);
                  if (compact != null) {
                    res = compact(res);
                  }
<<<<<<< Updated upstream
                  let putConfig = {key: key+'_res', gid: gid};
                  global.distribution.local.store.put(res,
                      putConfig, (e, v) => {
                        const deleteConfig = {key: key, gid: gid};
                        global.distribution.local.store.del(deleteConfig, (e, v) => {
                          count++;
                          if (count === matchedKeys.length) {
                            callback(e, allRes);
                          }
=======
                  // console.log('Map res: ', key, value, Array.isArray(res), res);
                  if (Array.isArray(res) && res.length == 0) {
                    const deleteConfig = {key: key, gid: gid, batch: batch};
                    global.distribution.local.store.del(deleteConfig, (e, v) => {
                      count++;
                      if (count === matchedKeys.length) {
                        callback(e, allRes);
                      }
                    });
                  } else {
                    let putConfig = {key: key+'_res', gid: gid, batch: batch};
                    global.distribution.local.store.put(res,
                        putConfig, (e, v) => {
                        // delete original store: '000'
                          const deleteConfig = {key: key, gid: gid,batch: batch};
                          global.distribution.local.store.del(deleteConfig, (e, v) => {
                            count++;
                            if (count === matchedKeys.length) {
                              callback(e, allRes);
                            }
                          });
>>>>>>> Stashed changes
                        });
                      });
                });
          }
        });
      };

<<<<<<< Updated upstream
      mrService.reducer = (keys, reduce, gid, callback) => {
        const storeFindConfig = {key: null, gid: gid};
        console.log('keys in reducer: ', keys);
=======
      mrService.reducer = (keys, reduce, gid, storeReducedValue, batch, callback) => {
        const storeFindConfig = {key: null, gid: gid, batch: batch};
        // console.log('keys in reducer: ', keys);
>>>>>>> Stashed changes
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
            callback(e, []);
          }
          let allRes = [];
          for (let key of matchedKeys) {
            const singleConfig = {key: key, gid: gid, batch: batch};
            global.distribution.local.store.get(singleConfig, (e, value) => {
              if (e != null) {
                callback(new Error('Local Store Get Error'), null);
              }

              let res = reduce(key, value);
              allRes.push(res);
<<<<<<< Updated upstream
              // console.log('Get result: ', res);
              count++;
              if (count === matchedKeys.length) {
                callback(e, allRes);
              }
=======

              if (!storeReducedValue) {
                count++;
                if (count === matchedKeys.length) {
                  callback(e, allRes);
                }
              } else {
                let putConfig = {key: key, gid: gid, batch: batch};
                const deleteConfig = {key: key, gid: gid, batch: batch};
                global.distribution.local.store.del(deleteConfig, (e, v) => {
                  global.distribution.local.store.put(res,
                      putConfig, (e, v) => {
                        count++;
                        if (count === matchedKeys.length) {
                          callback(e, allRes);
                        }
                      });
                });
              };
>>>>>>> Stashed changes
            });
          }
        });
      };

      mrService.shuffle = (keys, gid, batch, callback) => {
        const storeFindConfig = {key: null, gid: gid, batch: batch};
        keys = keys.map((item) => `${item}_res`);
        // console.log('Shuffle Keys: ', keys);

        global.distribution.local.store.get(storeFindConfig, (e, v) => {
          let matchedKeys = [];
          for (let i = 0; i < v.length; i++) {
            let key = v[i];
            if (keys.includes(key)) {
              matchedKeys.push(key);
            }
          }
          let count = 0;
          let groupCount = 0;
          let expectedCount = 0;
          let allKeys = [];
          if (matchedKeys.length === 0) {
            callback(e, allKeys);
          }
          // console.log('Shuffle Matched Keys: ', matchedKeys);
<<<<<<< Updated upstream
          for (let key of matchedKeys) {
            const singleConfig = {key: key, gid: gid};
            global.distribution.local.store.get(singleConfig, (e, value) => {
              global.distribution.local.store.del(singleConfig, () => {
                groupCount ++;
                // console.log('Shuffle Get: ', key, value);
                if (e != null) {
                  callback(new Error('Local Store Get Error'), null);
                } else if (Array.isArray(value)) {
                  expectedCount += value.length;
                  for (let obj of value) {
                    let keyList = Object.keys(obj);
                    const k = keyList[0];
                    allKeys.push(k);
                    const valueShuffle = obj[keyList[0]];
                    global.distribution[gid].store.append(valueShuffle,
                        k, (e, v) => {
                          count++;
                          if ((groupCount === matchedKeys.length) && (count === expectedCount)) {
                            callback(e, allKeys);
                          }
                        });
                  }
                } else {
                  let keyList = Object.keys(value);
                  const k = keyList[0];
                  if (k) {
                    allKeys.push(k);
                    const valueShuffle = value[keyList[0]];
                    global.distribution[gid].store.append(valueShuffle,
                        k, (e, v) => {
=======

          global.distribution[gid].status.get('nid', (e, nids) => {
            // console.log('Shuffle nids: ', nids);
            let nidList = Object.values(nids);
            const nodeSendList = {};
            for (let nid of nidList) {
              let sid = nid.substring(0, 5);
              nodeSendList[sid] = {};
            }
            distribution.local.groups.get(gid, (e, group) => {
              let sendToNodes = (nodeSendList, allKeys, callback) => {
                let count = 0;
                let keys = Object.keys(nodeSendList); // Get an array of keys

                for (let sid of keys) {
                  let node = group[sid];
                  let remote = {
                    node: node,
                    service: 'store',
                    method: 'appendAll',
                  };
                  global.distribution.local.comm.send([nodeSendList[sid], {gid: gid, batch:batch}], remote, () => {
                    count++;
                    if (count === keys.length) {
                      callback(e, allKeys);
                    }
                  });
                }
              };

              for (let key of matchedKeys) {
                const singleConfig = {key: key, gid: gid, batch: batch};
                global.distribution.local.store.get(singleConfig, (e, value) => {
                  global.distribution.local.store.del(singleConfig, () => {
                    if (e != null) {
                      callback(new Error('Local Store Get Error'), null);
                    } else if (Array.isArray(value)) {
                      // console.log('Shuffle value: ', value);
                      let countLevel2 = 0;
                      let expectedCount = value.length;
                      for (let obj of value) {
                        let keyList = Object.keys(obj);
                        const k = keyList[0];
                        allKeys.push(k);
                        const valueShuffle = obj[keyList[0]];
                        let kid = global.distribution.util.id.getID(k);
                        let nodeSID = global.config.hash(kid, nidList).substring(0, 5);
                        nodeSendList[nodeSID][k] = nodeSendList[nodeSID][k] || [];
                        nodeSendList[nodeSID][k].push(valueShuffle);
                        countLevel2++;
                        if (countLevel2 === expectedCount) {
>>>>>>> Stashed changes
                          count++;
                          if (count === matchedKeys.length) {
                            callback(e, allKeys);
                          }
                        });
                  } else {
                    count++;
                    if (count === matchedKeys.length) {
                      callback(e, allKeys);
                    }
                  }
                }
              });
            });
          }
        });
      };

      const mrServiceName = 'mr_' + id.getID(mrService);
      let total = 0;
      global.distribution[context.gid].routes.put(mrService,
          mrServiceName, (e, v) => {
            console.log('Finish Setup!', e, v);
            let remote = {
              service: mrServiceName,
              method: 'mapper',
            };
            let message = [keys, map, compact, context.gid, batch];
            global.distribution[context.gid].comm.send(message,
                remote, (e, v) => {
<<<<<<< Updated upstream
                  console.log('Finish Map!', e, v);
                  let message = [keys, context.gid];
=======
                  // console.log('Finish Map!', e, v);
                  let message = [keys, context.gid, batch];
>>>>>>> Stashed changes
                  let remote = {
                    service: mrServiceName,
                    method: 'shuffle',
                  };
                  global.distribution[context.gid].comm.send(message,
                      remote, (e, keyMap) => {
                        // console.log('Get shuffle value: ', keyMap);
                        let keySet = new Set();
                        for (let keys of Object.values(keyMap)) {
                          for (let key of keys) {
                            keySet.add(key);
                          }
                        }
                        // console.log('KeySet: ', keySet);
<<<<<<< Updated upstream
                        let message = [[...keySet], reduce, context.gid];
=======
                        let message = [[...keySet], reduce, context.gid, storeReducedValue, batch];
>>>>>>> Stashed changes
                        let remote = {
                          service: mrServiceName,
                          method: 'reducer',
                        };
                        global.distribution[context.gid].comm.send(message,
                            remote, (e, res) => {
                              let finalRes = [];
                              for (let resList of Object.values(res)) {
                                for (let r of resList) {
                                  if (r) {
                                    total += 1;
                                    finalRes.push(r);
                                  }
                                }
                              }
                              // console.log('Final res: ', finalRes);
                              callback(null, finalRes);
                            });
                      });
                });
          });
          console.log('-------total count:', total);
    },
  };
};

module.exports = mr;
