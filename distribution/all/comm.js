const local = require('../local/local');

const comm = (config) => {
  let context = {};
  context.gid = config.gid || 'all';

  return {
    send: (args, remote, callback) => {
      local.groups.get(context.gid, (e, nodes) => {
        let counter = 0;
        const errors = {};
        const values = {};

        for (let sid of Object.keys(nodes)) {
          remote.node = nodes[sid];
          local.comm.send(args, remote, (e, v) => {
            if (e) {
              errors[sid] = e;
            } else {
              values[sid] = v;
            }

            counter += 1;
            if (counter === Object.keys(nodes).length) {
              callback(errors, values);
            }
          });
        }
      });
    },
  };
};

module.exports = comm;
