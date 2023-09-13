const obj1 = ["used ", "and ", "extract "];
const obj2 = ["create ", "three", "extract ", "displays "];

const mergedKeys = [...new Set([...Object.keys(obj1), ...Object.keys(obj2)])];

console.log(mergedKeys);

["used ", "and ", "create ", "three", "extract ", "displays "];
