const arr = Array(300000);

console.log("arr: ", arr);
const startTime = new Date().getTime(); // Get the current time in milliseconds

arr.map((index, n) => {
  console.log(n);
});

const endTime = new Date().getTime(); // Get the current time after the for loop
const elapsedTime = endTime - startTime; // Calculate the elapsed time in milliseconds

console.log(`Time taken by the for loop: ${elapsedTime} milliseconds`);
