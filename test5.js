let obj = { a: 1, b: 2, c: 3 };

let a, b, c;
({ a, b, c } = obj);

console.log(a); // 1
console.log(b); // 2
console.log(c); // 3
