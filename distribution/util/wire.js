const serialization = require('./serialization');
const id = require('./id');

// store passed functions
global.toLocal = new Map(); // (funcID -> func)

function createRPC(func) {
  const serialedFunc = serialization.serialize(func);
  const funcID = id.getID(serialedFunc);
  global.toLocal.set(funcID, func);

  let stub = `
    const callback = args.pop() || function() {};

    let remote =  {node: ${JSON.stringify(global.nodeConfig)}, service: '${funcID}', method: 'call'};
    distribution.local.comm.send(args, remote, (e,v)=>{
        if (e) {
            callback(e);
        }
        else {
            callback(null,v)
        };
    });`;
  return new Function('...args',stub);
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