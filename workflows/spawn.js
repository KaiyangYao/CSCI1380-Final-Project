const distribution = require('../distribution');
const n2 = {ip: '172.31.14.64', port: 7111};
const startNodes = (cb) => {
  distribution.local.status.spawn(n2, (e, v) => {
    cb();
  });
};
let localServer = null;

distribution.node.start((server) => {
    localServer = server;
    startNodes(() => {
        console.log('node started!!');
    });
});