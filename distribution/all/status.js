let local = require('../local/local');

let status = (config) => {
  let context = {};
  context.gid = config.gid || 'all';
  return {
    get: (configuration, callback) => {
      callback = callback || function() {};
      global.distribution[context.gid].comm.send(
          [configuration], {service: 'status', method: 'get'}, (e, v) => {
            if (configuration === 'heapTotal' || configuration === 'heapUsed') {
              let sum = 0;
              Object.keys(v).forEach((sid) => {
                sum += v[sid];
              });
              callback(e, sum);
            } else {
              callback(e, v);
            }
          },
      );
    },
    stop: (callback) => {
      global.distribution[context.gid].comm.send(
          null, {service: 'status', method: 'stop'}, callback,
      );
    },
    spawn: (conf, callback) => {
      local.status.spawn(conf, (e, v) => {
        callback(e, v);
        local.groups.add(context.gid, conf, () => {});
        global.distribution[context.gid].comm.send(
            [context.gid, conf], {service: 'groups', method: 'add'}, () => {},
        );
      });
    },
  };
};

module.exports = status;
