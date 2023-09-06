function flattenObject(obj, parentKey = "") {
  let result = {};

  for (const key in obj) {
    if (typeof obj[key] === "object" && obj[key] !== null) {
      if (typeof obj[key] === "object" && isNaN(key)) {
        result[key] = null;
      }
      const flattened = flattenObject(obj[key], parentKey + key);
      for (const subKey in flattened) {
        result[parentKey + subKey] = flattened[subKey];
      }
    } else {
      result[parentKey + key] = obj[key];
      //   result[parentKey + key + key] = null; // Include null for non-object properties
    }
  }

  return result;
}

module.exports = flattenObject;
