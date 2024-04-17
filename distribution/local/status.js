const id = require('../util/id');
const wire = require('../util/wire');
const {serialize} = require('../util/serialization.js');
const {spawn} = require('node:child_process');
const path = require('node:path');

const status = {};

global.moreStatus = {
  sid: id.getSID(global.nodeConfig),
  nid: id.getNID(global.nodeConfig),
  counts: 0,
};

status.get = function(configuration, callback) {
  callback = callback || function() {};

  if (configuration in global.nodeConfig) {
    callback(null, global.nodeConfig[configuration]);
  } else if (configuration in moreStatus) {
    callback(null, moreStatus[configuration]);
  } else if (configuration === 'heapTotal') {
    callback(null, process.memoryUsage().heapTotal);
  } else if (configuration === 'heapUsed') {
    callback(null, process.memoryUsage().heapUsed);
  } else {
    callback(new Error('Status key not found'));
  }
};

status.stop = function(callback) {
  callback = callback || function() {};
  callback(null, global.nodeConfig);
  process.exit(0);
};

status.spawn = function(conf, callback) {
  callback = callback || function() {};
  if (conf.onStart === undefined) {
    conf.onStart = () => {};
  }
  const funcStr = `
  let onStart = ${conf.onStart.toString()};
  let callbackRPC = ${wire.createRPC(wire.toAsync(callback)).toString()};
  onStart();
  callbackRPC(null, global.nodeConfig, () => {});
  `;
  conf.onStart = new Function(funcStr);
  spawn(
      'node',
      [path.join(__dirname, '../../distribution.js'), '--config', serialize(conf)],
  );
};

module.exports = status;
