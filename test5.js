// Create a JavaScript Date object for the current date and time
const currentDate = new Date();

// Set the UTC time to 7:30
currentDate.setUTCHours(7, 30, 0, 0);

currentDate.setMinutes

console.log("Current date and time with UTC time set to 7:30:", currentDate.toISOString());
