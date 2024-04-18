//  ________________________________________
// / NOTE: You should use absolute paths to \
// | make sure they are agnostic to where   |
// | your code is running from! Use the     |
// \ `path` module for that purpose.        /
//  ----------------------------------------
//         \   ^__^
//          \  (oo)\_______
//             (__)\       )\/\
//                 ||----w |
//                 ||     ||

const node = global.nodeConfig;
const id = require('../util/id');
const fs = require('fs');
const path = require('path');
const serialization = require('../util/serialization');

const store = {};

const basePath = path.join(__dirname, '../../store');
if (!fs.existsSync(basePath)) {
  fs.mkdirSync(basePath);
}

const dirPath = path.join(__dirname, '../../store', id.getSID(node));
if (!fs.existsSync(dirPath)) {
  fs.mkdirSync(dirPath);
}

function sanitizeKey(key) {
  // Replace any characters that are not alphanumeric
  // or underscore with an empty string
  console.log('SANITIZE: ' + key);
  return key.replace(/[^a-zA-Z0-9_-]/g, '');
}

function getConfig(config, object) {
  let configuration = {};
  if (typeof config === 'string') {
    configuration.key = sanitizeKey(config);
    configuration.gid = 'local';
  } else if (typeof config === 'object') {
    configuration = config || {};
  }
  if (object) {
    configuration.key = configuration.key || id.getID(object);
  }
  configuration.gid = configuration.gid || 'local';
  return configuration;
}

function createGroupFolder(groupName) {
  if (!groupName) return;
  const dirPath = path.join(__dirname, '../../store', id.getSID(node),
      groupName);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath);
  }
}

store.put = function(object, config, callback) {
  callback = callback || function() {};

  config = getConfig(config, object);
  createGroupFolder(config.gid);
  const filePath = path.join(__dirname, '../../store', id.getSID(node),
      config.gid, sanitizeKey(config.key));
  fs.writeFileSync(filePath, serialization.serialize(object));
  callback(null, object);
};

store.append = function(object, config, callback) {
  // console.log('Object: ', object, config);
  callback = callback || function() {};
  config = getConfig(config, object);
  createGroupFolder(config.gid);
  const filePath = path.join(__dirname, '../../store', id.getSID(node),
      config.gid, sanitizeKey(config.key));
  if (fs.existsSync(filePath)) {
    const content = fs.readFileSync(filePath, 'utf8');
    const contentList = serialization.deserialize(content);
    contentList.push(object);
    fs.writeFileSync(filePath, serialization.serialize(contentList));
    callback(null, contentList);
  } else {
    const contentList = [object];
    fs.writeFileSync(filePath, serialization.serialize(contentList));
    callback(null, contentList);
  }
};

store.get = function(config, callback) {
  callback = callback || function() {};
  config = getConfig(config);
  createGroupFolder(config.gid);

  if (config.key == null) {
    const filePath = path.join(__dirname, '../../store', id.getSID(node),
        config.gid);
    fs.readdir(filePath, (err, files) => {
      if (err) {
        console.log(err);
      }
      console.log('LOCAL STORE GET: ', files);
      callback(null, files);
    });
  } else {
    const filePath = path.join(__dirname, '../../store', id.getSID(node),
        config.gid, sanitizeKey(config.key));
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, 'utf8');
      callback(null, serialization.deserialize(content));
    } else {
      callback(new Error('Invalid Name'), null);
    }
  }
};

store.del = function(config, callback) {
  callback = callback || function() {};
  config = getConfig(config);
  createGroupFolder(config.gid);

  const filePath = path.join(__dirname, '../../store', id.getSID(node),
      config.gid, sanitizeKey(config.key));
  if (fs.existsSync(filePath)) {
    const content = fs.readFileSync(filePath, 'utf8');
    fs.unlinkSync(filePath);
    callback(null, serialization.deserialize(content));
  } else {
    callback(new Error('Invalid Name'), null);
  }
};

module.exports = store;
