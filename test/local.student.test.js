let distribution;
let local;

let routes;
let comm;
// let node;

let lastPort = 8080;

beforeEach(() => {
  jest.resetModules();

  global.config = {
    ip: '127.0.0.1',
    port: lastPort++, // Avoid port conflicts
  };

  distribution = require('../distribution');
  local = distribution.local;

  status = local.status;
  routes = local.routes;
  comm = local.comm;
  messageCount = local.messageCount;

  id = distribution.util.id;
  wire = distribution.util.wire;

  node = global.config;
});

test('(1 pt) routes.put overwrite serviceName', (done) => {
  const firstService = {test: () => 'first service'};
  const secondService = {test: () => 'second service'};

  routes.put(firstService, 'testService', (e, v) => {
    routes.put(secondService, 'testService', (e, v) => {
      expect(e).toBeFalsy;
      routes.get('testService', (e, v) => {
        expect(e).toBeFalsy();
        expect(v.test()).toBe('second service');
        done();
      });
    });
  });
});

test('(1 pt) comm.send with multiple messages', (done) => {
  const message1 = ['ip'];
  const message2 = ['port'];

  const remote = {node: node, service: 'status', method: 'get'};

  distribution.node.start((server) => {
    comm.send(message1, remote, (e, v) => {
      expect(e).toBeFalsy();
      expect(v).toBe(node.ip);
    });
    comm.send(message2, remote, (e, v) => {
      server.close();
      expect(e).toBeFalsy();
      expect(v).toBe(node.port);
      done();
    });
  });
});

test('(1 pt) comm.send with invalid remote object', (done) => {
  const message = ['test'];
  const invalidRemote = {};

  comm.send(message, invalidRemote, (e, v) => {
    expect(e).toBeDefined();
    done();
  });
});

test('(1 pt) RPC with parameter basic', (done) => {
  let n = 0;

  const addOne = (x) => {
    n += x;
    return n;
  };

  const addOneRPC = distribution.util.wire.createRPC(
      distribution.util.wire.toAsync(addOne));

  const rpcService = {
    addOneRPC: addOneRPC,
  };

  distribution.node.start((server) => {
    routes.put(rpcService, 'rpcService', (e, v) => {
      routes.get('rpcService', (e, s) => {
        expect(e).toBeFalsy();
        s.addOneRPC(5, (e, v) => {
          server.close();
          expect(e).toBeFalsy();
          expect(v).toBe(5);
          done();
        });
      });
    });
  });
});

test('(1 pt) RPC with parameter', (done) => {
  let n = 0;

  const addOne = (x) => {
    n += x;
    return n;
  };

  const addOneRPC = distribution.util.wire.createRPC(
      distribution.util.wire.toAsync(addOne));

  const rpcService = {
    addOneRPC: addOneRPC,
  };

  distribution.node.start((server) => {
    routes.put(rpcService, 'rpcService', (e, v) => {
      routes.get('rpcService', (e, s) => {
        expect(e).toBeFalsy();
        s.addOneRPC(1, (e, v) => {
          s.addOneRPC(2, (e, v) => {
            s.addOneRPC(3, (e, v) => {
              server.close();
              expect(e).toBeFalsy();
              expect(v).toBe(6);
              done();
            });
          });
        });
      });
    });
  });
});

