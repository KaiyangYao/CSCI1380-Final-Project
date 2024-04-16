const id = require('../util/id');
const distribution = global.distribution;

let store = (config) => {
  let context = {};
  context.gid = config.gid || 'all'; // node group
  context.hash = config.hash || id.naiveHash; // hash functio
  return {
    get: (configuration, callback) => {
      if (!configuration) {
        let message = [{key: null, gid: context.gid}];
        distribution[context.gid].comm.send(message,
            {service: 'store', method: 'get'}, (errors, values) => {
              let ketList = Object.values(values).reduce((acc, val) =>
                acc.concat(val), []);
              callback(errors, ketList);
            });
      } else {
        let kid = id.getID(configuration);
        distribution[context.gid].status.get('nid', (e, nids) => {
          nids = Object.values(nids);
          let nid = context.hash(kid, nids);
          let sid = nid.substring(0, 5);

          distribution.local.groups.get(context.gid, (e, nodes) => {
            let node = nodes[sid];
            let remote = {service: 'store', method: 'get', node: node};
            let message = [{key: configuration, gid: context.gid}];
            distribution.local.comm.send(message, remote, callback);
          });
        });
      }
    },
    put: (object, configuration, callback) => {
      configuration = configuration || id.getID(object);
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        distribution.local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'store', method: 'put', node: node};
          let message = [object, {key: configuration, gid: context.gid}];
          distribution.local.comm.send(message, remote, callback);
        });
      });
    },
    append: (object, configuration, callback) => {
      configuration = configuration || id.getID(object);
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        distribution.local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'store', method: 'append', node: node};
          let message = [object, {key: configuration, gid: context.gid}];
          distribution.local.comm.send(message, remote, callback);
        });
      });
    },
    del: (configuration, callback) => {
      let kid = id.getID(configuration);
      distribution[context.gid].status.get('nid', (e, nids) => {
        nids = Object.values(nids);
        let nid = context.hash(kid, nids);
        let sid = nid.substring(0, 5);

        distribution.local.groups.get(context.gid, (e, nodes) => {
          let node = nodes[sid];
          let remote = {service: 'store', method: 'del', node: node};
          let message = [{key: configuration, gid: context.gid}];
          distribution.local.comm.send(message, remote, callback);
        });
      });
    },
    reconf: (groupName, sid, callback) => {
    },
  };
};

module.exports = store;
