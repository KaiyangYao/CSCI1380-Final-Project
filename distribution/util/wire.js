const id = require('../util/id');
const local = require('../local/local');

function createRPC(func) {
  let fpt = id.getID(func);
  let remote = {node: global.config, service: fpt, method: 'rpcPlaceHolder'};
  global.toLocal.set(fpt, func);

  const stub = (...args) => {
    const cb = args.pop() || function() {};
    local.comm.send(args, remote, function(e, v) {
      if (e) {
        cb(e, null);
      } else {
        cb(null, v);
      }
    });
  };

  return stub;
}

/*
    The toAsync function converts a synchronous function that returns a value
    to one that takes a callback as its last argument and returns the value
    to the callback.
*/
function toAsync(func) {
  return function(...args) {
    const callback = args.pop() || function() {};
    try {
      const result = func(...args);
      callback(null, result);
    } catch (error) {
      callback(error);
    }
  };
}

module.exports = {
  createRPC: createRPC,
  toAsync: toAsync,
};
