//  function demo() {
//   return new Promise((resolve, reject) => {
//     setTimeout(() => {
//       console.log("hiii");
//       resolve();
//     }, 3000);
//   });
// }

// async function main() {
//   await demo();
//   console.log("last line");
// }

// main();

const milliseconds = 90166995;
const hours = milliseconds / (1000 * 60 * 60);

const now = new Date();
now.setHours(now.getHours() + hours);

console.log(`The equivalent hours are: ${hours} hours`);
console.log(`The updated date and time are: ${now}`);
