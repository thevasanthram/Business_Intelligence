const obj = {
  1: { a: 1 },
  2: { a: 1 },
  3: { a: 1 },
};

const obj1 = [{ a: 1 }, { a: 1 }, { a: 1 }];

const arrayOfObjects = Object.keys(obj).map((key) => {
  return obj[key];
});

// const arr = Object.entries(obj);

console.log(arrayOfObjects);
