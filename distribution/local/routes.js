//const local = require('./local');
const comm = require('./comm');
const status = require('./status');
const groups = require('./groups');
const mem = require('./mem');
const store = require('./store');

const services = {};
const routes = {
  get: function(name, callback) {
    if (services.hasOwnProperty(name)) {
      callback(null, services[name]);
    } else if (global.toLocal.has(name)) {
      callback(null, {call: global.toLocal.get(name)});
    } else {
      callback(new Error('service not found'), null);
    }
  },

  put: function(service, name, callback) {
    services[name] = service;
    callback(null, 'service added');
  },
};

services['comm'] = comm;
services['routes'] = routes;
services['status'] = status;
services['groups'] = groups;
services['mem'] = mem;
services['store'] = store;

module.exports = routes;