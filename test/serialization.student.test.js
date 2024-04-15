const util = require('../distribution').util;

test('(1 pt) serializeBoolean', () => {
  const bool = false;
  const serialized = util.serialize(bool);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toBe(bool);
});

test('(1 pt) serializeCircularArray', () => {
  const arr = [1, 2, 3];
  arr.push(arr);
  const serialized = util.serialize(arr);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toEqual(arr);
});

test('(1 pt) serializeEmptyObject', () => {
  const obj = {};
  const serialized = util.serialize(obj);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toStrictEqual(obj);
});

test('(1 pt) serializeBuiltInConstructors', () => {
  const obj = String;
  const serialized = util.serialize(obj);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toBe(obj);
});

test('(1 pt) serializeNestedArray) ', () => {
  const complexArr = [1, 2, [3, 4, [5, 6]]];
  const serialized = util.serialize(complexArr);
  const deserialized = util.deserialize(serialized);
  expect(deserialized).toStrictEqual(complexArr);
});

test('(1 pt) serializeRainbowArray) ', () => {
  const originalArray = [
    1,
    'Hello, World!',
    [1, 2, 3, 4, 5],
    {x: 1, y: 2, z: 3},
    null,
    undefined,
  ];

  const serialized = util.serialize(originalArray);
  const deserializedArray = util.deserialize(serialized);
  expect(deserializedArray).toEqual(originalArray);
});
