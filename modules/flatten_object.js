function flattenObject(obj, parentKey = "") {
  let result = {};
  for (const key in obj) {
    if (typeof obj[key] === "object" && obj[key] !== null) {
      const flattened = flattenObject(obj[key], parentKey + key);
      result = { ...result, ...flattened };
    } else {
      result[parentKey + key] = obj[key];
    }
  }
  return result;
}

module.exports = flattenObject;
