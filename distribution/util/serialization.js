const objIdMap = new Map();
const idObjMap = new Map();
let id = 0;

const nativeIdMap = new Map();
const idNativeMap = new Map();
let nativeId = 0;

Reflect.ownKeys(globalThis).forEach((key) => {
  const value = globalThis[key];
  if (typeof value === 'function' && !nativeIdMap.has(value)) {
    nativeIdMap.set(value, nativeId);
    idNativeMap.set(nativeId, value);
    nativeId++;
  } else if (typeof value === 'object' && value !== null) {
    Reflect.ownKeys(value).forEach((subKey) => {
      const subValue = value[subKey];
      if (typeof subValue === 'function' && !nativeIdMap.has(subValue)) {
        nativeIdMap.set(subValue, nativeId);
        idNativeMap.set(nativeId, subValue);
        nativeId++;
      }
    });
  }
});

function serialize(object) {
  if (object === null) {
    return JSON.stringify({type: 'null', value: null});
  }

  if (typeof object === 'undefined') {
    return JSON.stringify({type: 'undefined', value: null});
  }

  const type = typeof object;
  if (type === 'number' || type === 'string' || type === 'boolean') {
    return JSON.stringify({type: type, value: object.toString()});
  }

  if (type === 'function') {
    if (nativeIdMap.has(object)) {
      return JSON.stringify({type: 'native', value: nativeIdMap.get(object)});
    }

    return JSON.stringify({type: 'function', value: object.toString()});
  }

  if (type === 'object') {
    if (objIdMap.has(object)) {
      return JSON.stringify({type: 'cycobj', value: objIdMap.get(object)});
    }

    objIdMap.set(object, id);
    idObjMap.set(id, object);
    id++;

    if (Array.isArray(object)) {
      return JSON.stringify({
        type: 'array',
        value: object.map((x) => serialize(x))});
    } else if (object instanceof Date) {
      return JSON.stringify({type: 'date', value: object.toISOString()});
    } else if (object instanceof Error) {
      return JSON.stringify({type: 'error', value: object.message});
    } else {
      const values = {};
      for (const key in object) {
        if (key) {
          values[key] = serialize(object[key]);
        }
      }
      return JSON.stringify({type: 'object', value: values});
    }
  }

  return '';
}

function deserialize(string) {
  const {type, value} = JSON.parse(string);
  switch (type) {
    case 'number':
      return Number(value);
    case 'string':
      return String(value);
    case 'boolean':
      return value === 'true'; // Boolean('false') = true! String is truthy
    case 'null':
      return null;
    case 'undefined':
      return undefined;
    case 'array':
      return value.map((x) => deserialize(x));
    case 'date':
      return new Date(value);
    case 'error':
      return new Error(value);
    case 'function':
      return new Function(`return ${value}`)();
    case 'object':
      const obj = {};
      for (const key in value) {
        if (key) {
          obj[key] = deserialize(value[key]);
        }
      }
      return obj;
    case 'cycobj':
      return idObjMap.get(value);
    case 'native':
      return idNativeMap.get(value);
  }
}

module.exports = {
  serialize: serialize,
  deserialize: deserialize,
};

