const currentDate = new Date();
const as_of_date = currentDate.toISOString().slice(0, 10);

console.log(as_of_date);
