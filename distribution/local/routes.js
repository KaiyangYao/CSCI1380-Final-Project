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
