function extractMatchingValues(obj1, obj2) {
  const matchingValues = {};

  for (const key in obj1) {
    matchingValues[key] = key in obj2 ? obj2[key] : null;
  }

  return matchingValues;
}

module.exports = extractMatchingValues;
