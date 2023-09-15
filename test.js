const sql = require("mssql");

// Create a table object with create option set to false
const table = new sql.Table("YourTableName");
table.create = false; // You don't need to define columns

// Sample data as an array of arrays
const data = [
  ["Value1", 123],
  ["Value2", 456],
  // Add more rows as needed
];

// Loop through the data and add rows to the table
data.forEach((row) => {
  table.rows.add(...row); // Spread the elements of the row array as arguments
});

// Perform the bulk insert
const request = new sql.Request();
request.bulk(table, (err, result) => {
  if (err) {
    console.error("Bulk insert error:", err);
  } else {
    console.log("Bulk insert completed:", result);
  }
});
