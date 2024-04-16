const http = require('http');

const serialization = require('../util/serialization');
const id = require('../util/id');

const node = global.config;
let messageCount = 0;
global.toLocal = new Map();

/*

Service  Description                           Methods
status   statusrmation about the current node  get
routes   A mapping from names to functions     get, put
comm     A message communication interface     send

*/

const status = {
  get: (key, callback) => {
    switch (key) {
      case 'nid':
        callback(null, id.getNID(node));
        break;
      case 'sid':
        callback(null, id.getSID(node));
        break;
      case 'ip':
        callback(null, node.ip);
        break;
      case 'port':
        callback(null, node.port);
        break;
      case 'counts':
        callback(null, messageCount);
        break;
      default:
        callback(new Error('Unknow key'), null);
    }
  },
};


const routes = {
  get: (serviceName, callback) => {
    if (services[serviceName]) {
      callback(null, services[serviceName]);
    } else if (toLocal.get(serviceName)) {
      callback(null, toLocal.get(serviceName));
    } else {
      callback(new Error('Service not registered'), null);
    }
  },

  put: (service, serviceName, callback) => {
    services[serviceName] = service;
    console.log(`Service '${serviceName}' registered successfully.`);

    if (callback) {
      callback(null, 'Success');
    }
  },
};

const comm = {
  send: (msg, remote, callback) => {
    // Validate the remote object
    if (!remote || !remote.node || !remote.node.ip || !remote.node.port) {
      const errorMessage = 'Invalid remote object: Missing required properties';
      if (callback) callback(new Error(errorMessage), null);
      return;
    }

    messageCount += 1;
    const data = serialization.serialize(msg);

    const options = {
      hostname: remote.node.ip,
      port: remote.node.port,
      path: `/${remote.service}/${remote.method}`,
      method: 'PUT',
      header: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
    };

    const req = http.request(options, (res) => {
      let resBody = '';

      res.on('data', (chunk) => {
        resBody += chunk;
      });

      res.on('end', () => {
        try {
          const resp = serialization.deserialize(resBody);
          callback(...resp);
        } catch (error) {
          callback(error, null);
        }
      });
    });

    req.write(data);
    req.end();
  },
};

const services = {
  'status': status,
  'routes': routes,
  'comm': comm,
};

module.exports = {
  status,
  routes,
  comm,
  messageCount,
};
