const fs = require("fs");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

// Define the CSV file path and column headers
const csvFilePath = "mydata.csv";
const csvWriter = createCsvWriter({
  path: csvFilePath,
  header: [
    { id: "name", title: "Name" },
    { id: "email", title: "Email" },
  ],
  append: true, // Set to true to append rows
});

// Sample data to append
const newData = [
  { name: "John Doe", email: "john@example.com" },
  { name: "Jane Smith", email: "jane@example.com" },
];

// Append the new data to the CSV file
csvWriter
  .writeRecords(newData)
  .then(() => {
    console.log("Data appended to CSV file.");
  })
  .catch((error) => {
    console.error("Error appending data:", error);
  });
